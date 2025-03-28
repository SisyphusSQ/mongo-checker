package task

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/juju/ratelimit"
	"go.mongodb.org/mongo-driver/bson"

	"mongo-checker/internal/config"
	"mongo-checker/internal/dao"
	"mongo-checker/internal/model/do"
	"mongo-checker/internal/model/dto"
	"mongo-checker/internal/progress"
	l "mongo-checker/pkg/log"
	mg "mongo-checker/pkg/mongo"
	"mongo-checker/utils"
)

type Task struct {
	sync.Mutex

	ctx    context.Context
	cancel context.CancelFunc

	sourceUrl string
	destUrl   string
	connMode  string

	secStepFin   atomic.Int32
	thirdStepFin atomic.Int32

	includeDBs   []string
	excludeDBs   []string
	includeColls []string
	excludeColls []string

	checkNS  map[dto.NS]dto.NS // destNs : srcNs
	checkDBs map[string]string // destDB : srcDB

	limit  int
	bucket *ratelimit.Bucket

	dbConn *dao.SqliteDao

	errChan  chan error
	parallel chan struct{}
	doneChan chan struct{}

	transRules map[string]string
	subTaskMp  map[dto.NS]*SubTask
}

type SubTask struct {
	checkChan chan *dto.CheckMsg
	retryChan chan *do.ResultRecord

	checkTask *progress.Checker
}

func New(cfg *config.Config) (*Task, error) {
	var (
		err   error
		rules = make(map[string]string)
		task  = &Task{
			sourceUrl: cfg.Source,
			destUrl:   cfg.Destination,
			connMode:  cfg.ConnectMode,

			limit:  cfg.LimitQPS,
			bucket: ratelimit.NewBucketWithQuantum(time.Second, int64(cfg.LimitQPS), int64(cfg.LimitQPS)),

			includeDBs:   make([]string, 0),
			excludeDBs:   make([]string, 0),
			includeColls: make([]string, 0),
			excludeColls: make([]string, 0),
			checkNS:      make(map[dto.NS]dto.NS),
			checkDBs:     make(map[string]string),

			errChan:  make(chan error),
			parallel: make(chan struct{}, cfg.Parallel),
			doneChan: make(chan struct{}),

			subTaskMp: make(map[dto.NS]*SubTask),
		}
	)
	task.ctx, task.cancel = context.WithCancel(context.Background())

	task.transRules = make(map[string]string)
	if cfg.DBTrans != "" {
		for _, trans := range strings.Split(cfg.DBTrans, ",") {
			dbs := strings.Split(trans, ":")
			if len(dbs) != 2 || dbs[0] == "" || dbs[1] == "" {
				return nil, fmt.Errorf("db translation format error, detail: %s", cfg.DBTrans)
			}
			rules[dbs[0]] = dbs[1]
		}
		task.transRules = rules
	}

	if cfg.IncludeDBs != "" {
		task.includeDBs = strings.Split(cfg.IncludeDBs, ",")
	} else if cfg.ExcludeDBs != "" {
		task.excludeDBs = strings.Split(cfg.ExcludeDBs, ",")
	}
	if cfg.IncludeColls != "" {
		task.includeColls = strings.Split(cfg.IncludeColls, ",")
	} else if cfg.ExcludeColls != "" {
		task.excludeColls = strings.Split(cfg.ExcludeColls, ",")
	}

	task.dbConn, err = dao.NewSqliteDao(cfg.LogPath)
	if err != nil {
		return nil, err
	}
	go task.loop()

	return task, nil
}

func (t *Task) loop() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			t.Lock()
			for ns, sb := range t.subTaskMp {
				if sb.checkTask == nil {
					// Perhaps checkTask hasn't inited yet
					continue
				}

				if !sb.checkTask.IsRunning() {
					// delete not running task
					delete(t.subTaskMp, ns)
				}

				m := sb.checkTask.Metric()
				l.Logger.Infof("[MONITOR] ns[%s] checkPercent[%.2f%%] processed[%d] total[%d] wrong[%d] notFound[%d]",
					m.Ns.Str(), float64(m.Processed)/float64(m.Total)*100, m.Processed, m.Total, m.WrongNum, m.NotFound)
			}
			t.Unlock()
		case <-t.ctx.Done():
			return
		}
	}
}

func (t *Task) Start() error {
	l.Logger.Info("mongo-checker task starting...")

	// [FIRST STEP] check whether ns is existed in destination
	l.Logger.Info("[FIRST STEP] start to check whether source ns is existed in destination")
	srcConn, err := mg.NewMongoConn(t.sourceUrl, t.connMode)
	if err != nil {
		l.Logger.Errorf("source[%s] can not connect to source database, %v",
			utils.BlockPassword(t.sourceUrl, "***"), err)
	}
	destConn, err := mg.NewMongoConn(t.destUrl, t.connMode)
	if err != nil {
		l.Logger.Errorf("dest[%s] can not connect to destination database, %v",
			utils.BlockPassword(t.destUrl, "***"), err)
	}

	err = t.checkAndSetNs(srcConn, destConn)
	if err != nil {
		l.Logger.Errorf("[FIRST STEP] checkAndSetDBs error, %v]", err)
		t.closeAll(destConn, srcConn)
		return err
	}

	t.closeAll(destConn, srcConn)
	l.Logger.Info("[FIRST STEP] NS in source and destination is equaled, go to [SECOND STEP]")

	// [SECOND STEP] will check documents whether is equal by two sides
	t.fullCheck()
	select {
	case err = <-t.errChan:
		return err
	case <-t.doneChan:
		l.Logger.Info("[SECOND STEP] all NS check tasks in source and destination are finished")
	}

	// [THIRD STEP] have wrong records, check it twice
	nss, err := t.dbConn.GetNeedCheckNS()
	if err != nil {
		l.Logger.Errorf("get need second check ns error, %v", err)
		return err
	}
	if len(nss) == 0 {
		goto End
	}

	t.twiceCheck(nss)
	select {
	case err = <-t.errChan:
		return err
	case <-t.doneChan:
		l.Logger.Info("[THIRD STEP] all NS check tasks in source and destination are finished")
	}

End:
	// if code reached here, all steps are completed
	return nil
}

func (t *Task) fullCheck() {
	// [SECOND STEP] will check documents whether is equal by two sides
	l.Logger.Info("[SECOND STEP] will check documents whether is equal by two sides")
	go func() {
		for destNs, srcNs := range t.checkNS {
			t.parallel <- struct{}{}
			l.Logger.Infof("[SECOND STEP] source:[%s] dest:[%s] is starting to check", srcNs.Str(), destNs.Str())

			t.Lock()
			t.subTaskMp[srcNs] = &SubTask{
				checkChan: make(chan *dto.CheckMsg),
				retryChan: make(chan *do.ResultRecord, t.limit),
			}
			sb := t.subTaskMp[srcNs]
			t.Unlock()

			go func() {
				defer func() { <-t.parallel }()

				mp := map[dto.NS]dto.NS{destNs: srcNs}
				ck, err := progress.NewChecker(t.ctx, mp, t.sourceUrl, t.destUrl, t.connMode, t.bucket,
					sb.checkChan, sb.retryChan, t.dbConn)
				if err != nil {
					l.Logger.Errorf("[SECOND STEP] ns[%s] create check task err, %v", srcNs.Str(), err)
					t.errChan <- err
				}
				sb.checkTask = ck

				err = ck.Start()
				if err != nil {
					t.errChan <- err
				}
				t.secStepFin.Add(1)
			}()
		}

		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				//l.Logger.Debugf("len(t.checkNS)=%d, t.secStepFin=%d)", len(t.checkNS), t.secStepFin.Load())
				if len(t.checkNS) == int(t.secStepFin.Load()) {
					t.doneChan <- struct{}{}
					return
				}
			case <-t.ctx.Done():
				return
			}
		}
	}()
}

func (t *Task) twiceCheck(nss []*do.Overview) {
	l.Logger.Info("[THIRD STEP] have wrong records, check it twice")
	go func() {
		var err error
		for _, n := range nss {
			t.parallel <- struct{}{}

			go func() {
				defer func() { <-t.parallel }()

				// src and dest must be existed
				var src, dest dto.NS
				for destNs, srcNs := range t.checkNS {
					if n.Database == srcNs.Database && n.Coll == srcNs.Collection {
						src = srcNs
						dest = destNs
						break
					}
				}

				err = t.twiceCompare(src, dest)
				if err != nil {
					l.Logger.Errorf("[THIRD STEP] twice compare error, %v", err)
					t.errChan <- err
				}
				t.thirdStepFin.Add(1)
			}()
		}

		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if len(nss) == int(t.thirdStepFin.Load()) {
					t.doneChan <- struct{}{}
				}
				return
			case <-t.ctx.Done():
				return
			}
		}
	}()
}

func (t *Task) twiceCompare(src, dest dto.NS) error {
	l.Logger.Infof("source:[%s] dest:[%s] is starting to check twice", src.Str(), dest.Str())

	t.Lock()
	t.subTaskMp[src] = &SubTask{
		checkChan: make(chan *dto.CheckMsg),
		retryChan: make(chan *do.ResultRecord, t.limit),
	}
	sb := t.subTaskMp[src]
	t.Unlock()

	mp := map[dto.NS]dto.NS{dest: src}
	ck, err := progress.NewChecker(t.ctx, mp, t.sourceUrl, t.destUrl, t.connMode, t.bucket,
		sb.checkChan, sb.retryChan, t.dbConn)
	if err != nil {
		l.Logger.Errorf("[THIRD STEP] ns[%s] create check task err, %v", src.Str(), err)
		return err
	}
	sb.checkTask = ck

	rows, cnt, err := t.dbConn.GetNSWrongRecord(src.Database, src.Collection)
	if err != nil {
		return err
	}

	if cnt > 1000 {
		return fmt.Errorf("ns[%s] has to mush wrong datas, stopping to check twice", src.Str())
	}

	for rows.Next() {
		t.bucket.Take(1)

		var r do.ResultRecord
		err = rows.Scan(&r)
		if err != nil {
			return err
		}

		err = ck.CheckOne(r.MID)
		if err != nil {
			return err
		}
	}

	// finished checking here
	metric := ck.Metric()
	updates := map[string]any{
		"wrong":     metric.WrongNum,
		"not_found": metric.NotFound,
	}
	err = t.dbConn.UpdateOverview(src, updates)
	return err
}

func (t *Task) Stop() {
	t.cancel()
}

func (t *Task) checkAndSetNs(srcConn, destConn *mg.Conn) error {
	srcDBs, err := srcConn.Client.ListDatabaseNames(t.ctx, bson.M{})
	if err != nil {
		l.Logger.Errorf("source[%s] can not list db names, %s",
			utils.BlockPassword(t.sourceUrl, "***"), err.Error())
		t.closeAll(srcConn, destConn)
		return err
	}
	destDBs, err := destConn.Client.ListDatabaseNames(t.ctx, bson.M{})
	if err != nil {
		l.Logger.Errorf("dest[%s] can not list db names, %s",
			utils.BlockPassword(t.destUrl, "***"), err.Error())
		t.closeAll(srcConn, destConn)
		return err
	}

	for _, srcDB := range srcDBs {
		if slices.Contains(t.excludeDBs, srcDB) {
			continue
		}

		if slices.Contains(t.includeDBs, srcDB) {
			if tranDB, ok := t.transRules[srcDB]; ok {
				t.checkDBs[tranDB] = srcDB
			} else {
				t.checkDBs[srcDB] = srcDB
			}
		}
	}

	var num int
	for _, destDB := range destDBs {
		if _, ok := t.checkDBs[destDB]; ok {
			num++
		}
	}
	if num != len(t.checkDBs) {
		return fmt.Errorf("source and destination databases don't match, source: %v, dest: %v", srcDBs, destDBs)
	}

	// set ns
	for ddb, sdb := range t.checkDBs {
		scols, err2 := srcConn.Client.Database(sdb).ListCollectionNames(t.ctx, bson.M{})
		if err2 != nil {
			l.Logger.Errorf("source db[%s] can not list collection names, %v", sdb, err2)
			t.closeAll(srcConn, destConn)
			return err
		}

		dcols, err2 := destConn.Client.Database(ddb).ListCollectionNames(t.ctx, bson.M{})
		if err2 != nil {
			l.Logger.Errorf("dest db[%s] can not list collection names, %v", ddb, err2)
			t.closeAll(srcConn, destConn)
			return err
		}

		var colLen int
		for _, dcol := range dcols {
			if slices.Contains(scols, dcol) {
				colLen++

				// we don't transform collection's name, so use dest coll name as source coll name
				t.checkNS[dto.NS{Database: ddb, Collection: dcol}] = dto.NS{Database: sdb, Collection: dcol}
			}
		}
		if colLen != len(scols) {
			return fmt.Errorf("source and destination collections don't match, source: %v, dest: %v", scols, dcols)
		}
	}
	return nil
}

func (t *Task) closeAll(conns ...*mg.Conn) {
	for _, conn := range conns {
		_ = conn.Close()
	}
}
