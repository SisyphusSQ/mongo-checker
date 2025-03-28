package progress

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/spf13/cast"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"

	"mongo-checker/internal/model/dto"
	l "mongo-checker/pkg/log"
	mg "mongo-checker/pkg/mongo"
)

type Extractor struct {
	wg  sync.WaitGroup
	ctx context.Context

	ns       dto.NS
	url      string // source
	connMode string

	conn *mg.Conn

	errChan   chan error
	checkChan chan<- *dto.CheckMsg
}

func NewExtractor(ctx context.Context, ns dto.NS, url, connMode string, checkChan chan *dto.CheckMsg) *Extractor {
	return &Extractor{
		ctx:       ctx,
		ns:        ns,
		url:       url,
		connMode:  connMode,
		checkChan: checkChan,
		errChan:   make(chan error),
	}
}

func (e *Extractor) NSCount() (int64, error) {
	conn, err := mg.NewMongoConn(e.url, e.connMode)
	if err != nil {
		l.Logger.Errorf("Failed to connect to MongoDB: %v", err)
		return 0, err
	}
	defer conn.Close()

	var res bson.M
	err = conn.Client.Database(e.ns.Database).
		RunCommand(e.ctx, bson.D{{"collStats", e.ns.Collection}}).
		Decode(&res)
	if err != nil {
		l.Logger.Errorf("Failed to get ns[%s] stats: %v", e.ns.Str(), err)
		return 0, err
	}

	if cnt, ok := res["count"]; ok {
		return cast.ToInt64(cnt), nil
	}
	return 0, fmt.Errorf("ns[%s] hasn't got count, stats: %v", e.ns.Str(), res)
}

func (e *Extractor) Stream(wg *sync.WaitGroup) error {
	wg.Add(1)
	defer wg.Done()

	l.Logger.Infof("Extractor is starting to extract streaming data from ns[%s]", e.ns.Str())
	conn, err := mg.NewMongoConn(e.url, e.connMode)
	if err != nil {
		l.Logger.Errorf("Failed to connect to MongoDB: %v", err)
		return err
	}
	defer conn.Close()

	cur, err2 := conn.Client.Database(e.ns.Database).Collection(e.ns.Collection).
		Find(e.ctx, bson.M{})
	if err2 != nil {
		l.Logger.Errorf("Failed to get data from ns[%s]: %v", e.ns.Str(), err2)
	}
	defer cur.Close(e.ctx)

	for cur.Next(e.ctx) {
		var data bson.M
		err = cur.Decode(&data)
		if err != nil {
			l.Logger.Errorf("Ns[%s] query cursor got err: %v", e.ns.Str(), err)
			return err
		}

		id, ok := data["_id"]
		if !ok {
			e.errChan <- fmt.Errorf("ns[%s] query cursor got no _id", e.ns.Str())
		}

		e.checkChan <- &dto.CheckMsg{
			Id:  id,
			Raw: cur.Current,
		}
	}

	e.checkChan <- &dto.CheckMsg{
		Done: true,
	}

	if err = cur.Err(); err != nil {
		if errors.Is(err, context.Canceled) {
			l.Logger.Warnf("Ns[%s] context is canceled", e.ns.Str())
			return nil
		}

		l.Logger.Errorf("Ns[%s] query cursor got err: %v", e.ns.Str(), err)
		return err
	}
	return nil
}

func (e *Extractor) FindOne(id string) error {
	// todo: 后续适配除objectId之外的主键类型
	pid, err := primitive.ObjectIDFromHex(id)
	if err != nil {
		return err
	}

	// only set e.conns here
	if e.conn == nil {
		e.conn, err = mg.NewMongoConn(e.url, e.connMode)
		if err != nil {
			l.Logger.Errorf("Failed to connect to MongoDB: %v", err)
			return err
		}
	}

	raw, err := e.conn.Client.Database(e.ns.Database).Collection(e.ns.Collection).
		FindOne(e.ctx, bson.M{"_id": id}).DecodeBytes()
	if err != nil {
		l.Logger.Errorf("Failed to get data from ns[%s]: %v", e.ns.Str(), err)
		return err
	}

	e.checkChan <- &dto.CheckMsg{
		Id:  pid,
		Raw: raw,
	}
	return nil
}

func (e *Extractor) Stop() error {
	// if e.conns is not nil, it must have wrong value, and new Checker to retry check data
	if e.conn != nil {
		e.conn.Close()

		e.checkChan <- &dto.CheckMsg{
			Done: true,
		}
	}
	close(e.errChan)

	l.Logger.Debugf("Extractor ns[%s] is stopped", e.ns.Str())
	return nil
}
