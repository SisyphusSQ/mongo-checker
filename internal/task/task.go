package task

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/juju/ratelimit"
	"go.mongodb.org/mongo-driver/bson"

	"mongo-checker/internal/config"
	"mongo-checker/internal/model/do"
)

type Task struct {
	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc

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
		task  = new(Task)
		rules = make(map[string]string)
	)
	task.ctx, task.cancel = context.WithCancel(context.Background())

	if cfg.DBTrans != "" {
		for _, trans := range strings.Split(cfg.DBTrans, ",") {
			dbs := strings.Split(trans, ":")
			if len(dbs) != 2 || dbs[0] == "" || dbs[1] == "" {
				return nil, fmt.Errorf("db translation format error, detail: %s", cfg.DBTrans)
			}
			rules[dbs[0]] = dbs[1]
		}
	}

	return task, nil
}

func RunTask(cfg *config.Config) error {

	return nil
}
