package progress

import (
	"context"
	"sync"

	"go.mongodb.org/mongo-driver/bson"
	"mongo-checker/internal/model/dto"
)

type Checker struct {
	wg  sync.WaitGroup
	ctx context.Context

	ns       dto.NS
	url      string // source
	connMode string

	checkChan    chan<- bson.M
	retryChan    <-chan bson.M
	overviewChan <-chan bson.M
}

/*
a := bson.Raw{}
	b := bson.Raw{}
	bytes.Equal(a,b)
*/
