package progress

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"github.com/juju/ratelimit"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"

	"mongo-checker/internal/dao"
	"mongo-checker/internal/model/do"
	"mongo-checker/internal/model/dto"
	l "mongo-checker/pkg/log"
	mg "mongo-checker/pkg/mongo"
)

type Checker struct {
	wg sync.WaitGroup

	ctx     context.Context
	pCtx    context.Context
	pCancel context.CancelFunc

	ns       dto.NS
	url      string // destination
	connMode string

	isRunning bool
	bucket    *ratelimit.Bucket

	retryTime int
	retryWait time.Duration
	dbConn    *dao.SqliteDao

	clientNum int
	conns     map[int]*mg.Conn
	cols      map[int]*mongo.Collection
	extractor *Extractor

	docParall chan struct{}
	doneChan  chan struct{}

	// metrics
	total     int64
	processed int64
	wrongNum  atomic.Int64
	notFound  atomic.Int64

	errChan   chan error
	checkChan chan *dto.CheckMsg
	retryChan chan *do.ResultRecord
}

// NewChecker checker will include extractor
// so new extractor when new checker
func NewChecker(ctx context.Context, nsMp map[dto.NS]dto.NS, src, dest, connMode string, bucket *ratelimit.Bucket,
	checkChan chan *dto.CheckMsg, retryChan chan *do.ResultRecord, dbConn *dao.SqliteDao) (*Checker, error) {

	c := &Checker{
		ctx:       ctx,
		url:       dest,
		connMode:  connMode,
		checkChan: checkChan,

		dbConn:    dbConn,
		isRunning: true,
		bucket:    bucket,

		clientNum: 5,

		retryChan: retryChan,
		retryWait: time.Second,
		retryTime: 3,

		errChan: make(chan error, 5),
		conns:   make(map[int]*mg.Conn),
		cols:    make(map[int]*mongo.Collection),

		docParall: make(chan struct{}, bucket.Capacity()),
		doneChan:  make(chan struct{}),
	}
	c.pCtx, c.pCancel = context.WithCancel(ctx)

	if len(nsMp) != 1 {
		return nil, fmt.Errorf("nsMp len should be 1")
	}
	var destNs dto.NS
	for k, v := range nsMp {
		destNs = k
		c.ns = v
	}

	for n := range c.clientNum {
		destConn, err := mg.NewMongoConn(c.url, c.connMode)
		if err != nil {
			return nil, err
		}
		c.conns[n] = destConn
		c.cols[n] = destConn.Client.Database(c.ns.Database).Collection(c.ns.Collection)
	}

	// new extractor
	e := NewExtractor(c.pCtx, destNs, src, connMode, checkChan)
	docCnt, err := e.NSCount()
	if err != nil {
		return nil, err
	}
	c.total = docCnt
	c.extractor = e

	go func() {
		c.wg.Add(1)
		defer c.wg.Done()
		c.errChan <- c.checkLoop()
	}()
	return c, nil
}

func (c *Checker) Start() error {
	go func() {
		c.wg.Add(1)
		defer c.wg.Done()
		c.errChan <- c.extractor.Stream(&c.wg)
	}()
	var (
		ticker  = time.NewTicker(5 * time.Second)
		records = make([]*do.ResultRecord, 0)
	)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			err := c.writeRecords(records)
			if err != nil {
				c.errChan <- err
			}
			records = make([]*do.ResultRecord, 0)
		case record := <-c.retryChan:
			records = append(records, record)
			if len(records) >= 50 {
				err := c.writeRecords(records)
				if err != nil {
					c.errChan <- err
				}
				records = make([]*do.ResultRecord, 0)
			}
		case <-c.doneChan:
			l.Logger.Infof("check ns[%s] task is finished or canceled", c.ns.Str())
			// write ns overview to sqlite
			ov := do.Overview{
				Database:   c.ns.Database,
				Coll:       c.ns.Collection,
				NotFound:   c.notFound.Load(),
				Wrong:      c.wrongNum.Load(),
				Total:      c.total,
				CheckTotal: c.processed,
			}
			err := c.dbConn.CreateOverview(ov)
			if err != nil {
				l.Logger.Errorf("check ns[%s] create overview err: %v", c.ns.Str(), err)
			}

			return c.Stop()
		case err := <-c.errChan:
			if err != nil {
				l.Logger.Errorf("checker ns[%s] error: %v", c.ns.Str(), err)
				c.Stop()
				return err
			}
		case <-c.ctx.Done():
			l.Logger.Infof("[checker] ns[%s] context done", c.ns.Str())
			return c.Stop()
		}
	}
}

func (c *Checker) checkLoop() error {
	l.Logger.Infof("starting to check ns[%s]", c.ns.Str())
	for {
		select {
		case msg, ok := <-c.checkChan:
			if !ok {
				l.Logger.Warnf("check ns[%s] task chan is closed", c.ns.Str())
				return nil
			}

			c.bucket.Take(1)
			if msg.Done {
				return c.waitForDone()
			}

			c.processed++
			c.docParall <- struct{}{}
			go func() {
				defer func() {
					<-c.docParall
				}()

				res, destRaw, err := c.compare(0, msg)
				if err != nil {
					l.Logger.Warnf("check ns[%s] err: [%v]; msg: %v", c.ns.Str(), err, msg)
					c.errChan <- err
				}
				if res == dto.Equal {
					return
				}

				// not consider res == Error, error is handled before
				if res == dto.Wrong {
					c.wrongNum.Add(1)
				} else if res == dto.NotFound {
					c.notFound.Add(1)
				}

				// todo: 目前先只支持 _id为objectId类型的数据
				var mid string
				if v, ok := msg.Id.(primitive.ObjectID); ok {
					mid = v.Hex()
				} else {
					panic(fmt.Errorf("_id should be ObjectID, detail: %v", msg.Id))
				}

				c.retryChan <- &do.ResultRecord{
					Database:  c.ns.Database,
					Coll:      c.ns.Collection,
					SeqNum:    dto.First,
					MID:       mid,
					SrcBson:   string(msg.Raw),
					DestBson:  string(destRaw),
					WrongType: string(res),
				}
			}()
		case <-c.ctx.Done():
			l.Logger.Infof("check loop[%s] is asked to cancel", c.ns.Str())
			c.doneChan <- struct{}{}
			return nil
		}
	}
}

func (c *Checker) waitForDone() error {
	tk := time.NewTicker(500 * time.Millisecond)
	defer tk.Stop()

	for {
		select {
		case <-tk.C:
			if c.processed >= c.total {
				l.Logger.Infof("check ns[%s] task chan is done", c.ns.Str())
				c.doneChan <- struct{}{}
				return nil
			}
		case <-c.doneChan:
			return nil
		}
	}
}

func (c *Checker) CheckOne(id string) error {
	err := c.extractor.FindOne(id)
	if err != nil {
		return err
	}

	time.Sleep(40 * time.Millisecond)
	select {
	case record := <-c.retryChan:
		record.SeqNum = dto.Second
		err = c.writeRecords([]*do.ResultRecord{record})
		if err != nil {
			return err
		}
	case err = <-c.errChan:
		return err
	default:
	}

	return nil
}

func (c *Checker) compare(retry int, msg *dto.CheckMsg) (dto.CheckRes, bson.Raw, error) {
	var (
		res dto.CheckRes
		num = rand.IntN(c.clientNum)
	)

	//l.Logger.Debugf("ns[%s] get cols[%d]", c.ns.Str(), num)
	destRaw, err := c.cols[num].FindOne(c.ctx, bson.M{"_id": msg.Id}).DecodeBytes()
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			res = dto.NotFound
			goto Problem
		}

		l.Logger.Errorf("Failed to get data from dest ns[%s]: %v", c.ns.Str(), err)
		return dto.Error, nil, err
	}

	if bytes.Equal(msg.Raw, destRaw) {
		return dto.Equal, nil, nil
	}
	res = dto.Wrong

Problem:
	// recursion retry here
	if retry >= c.retryTime {
		return res, destRaw, nil
	}

	sleep := (time.Duration(rand.Int64N(int64(c.retryWait)))) / 2
	time.Sleep(sleep)
	retry++
	return c.compare(retry, msg)
}

func (c *Checker) writeRecords(records []*do.ResultRecord) error {
	if len(records) == 0 {
		return nil
	}

	err := c.dbConn.CreateResultRecord(records)
	if err != nil {
		l.Logger.Errorf("ns[%s] create result record failed: %v", c.ns.Str(), err)
		return err
	}
	l.Logger.Debugf("ns[%s] create result record success, len: %d", c.ns.Str(), len(records))
	return nil
}

func (c *Checker) Metric() dto.Metric {
	return dto.Metric{
		Ns:        c.ns,
		Total:     c.total,
		Processed: c.processed,
		WrongNum:  c.wrongNum.Load(),
		NotFound:  c.notFound.Load(),
	}
}

func (c *Checker) IsRunning() bool {
	return c.isRunning
}

func (c *Checker) Stop() error {
	c.pCancel()
	c.extractor.Stop()
	for n := range c.clientNum {
		c.conns[n].Close()
	}
	c.wg.Wait()

	close(c.doneChan)
	close(c.errChan)
	close(c.checkChan)
	c.isRunning = false

	l.Logger.Debugf("Checker ns[%s] is stopped", c.ns.Str())
	return nil
}
