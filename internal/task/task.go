package task

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/juju/ratelimit"
	"go.mongodb.org/mongo-driver/bson"

	"mongo-checker/internal/config"
	"mongo-checker/internal/model/do"
	"mongo-checker/internal/model/dto"
	l "mongo-checker/pkg/log"
	mg "mongo-checker/pkg/mongo"
	"mongo-checker/utils"
)

type Task struct {
	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc

	isRunning bool
	sourceUrl string
	destUrl   string
	connMode  string

	includeDBs   []string
	excludeDBs   []string
	includeColls []string
	excludeColls []string

	checkNS  map[dto.NS]dto.NS // destNs : srcNs
	checkDBs map[string]string // destDB : srcDB

	limit  int
	bucket *ratelimit.Bucket

	errChan      chan error
	overviewChan chan do.Overview
	checkMpChan  map[string]chan bson.M
	retryMpChan  map[string]chan bson.M

	transRules map[string]string
}

func (t *Task) New(cfg *config.Config) (*Task, error) {
	var (
		rules = make(map[string]string)
		task  = &Task{
			includeDBs:   make([]string, 0),
			excludeDBs:   make([]string, 0),
			includeColls: make([]string, 0),
			excludeColls: make([]string, 0),
			checkNS:      make(map[dto.NS]dto.NS),
			checkDBs:     make(map[string]string),
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

	task.limit = cfg.ChunkSize
	task.bucket = ratelimit.NewBucketWithQuantum(1*time.Millisecond, 1, 1)
	task.errChan = make(chan error)
	task.overviewChan = make(chan do.Overview)
	task.checkMpChan = make(map[string]chan bson.M)
	task.retryMpChan = make(map[string]chan bson.M)

	task.sourceUrl = cfg.Source
	task.destUrl = cfg.Destination
	task.connMode = cfg.ConnectMode
	return task, nil
}

func (t *Task) Start() error {
	l.Logger.Info("mongo-checker task starting...")

	// firstly check whether ns is existed in destination
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

	return nil
}

func (t *Task) Stop() {
	t.isRunning = false
	t.cancel()
	t.wg.Wait()
}

func (t *Task) IsRunning() bool {
	return t.isRunning
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
