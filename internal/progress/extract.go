package progress

import (
	"context"
	"sync"

	"github.com/juju/ratelimit"
	"go.mongodb.org/mongo-driver/bson"

	"mongo-checker/internal/model/dto"
)

type Extractor struct {
	wg  sync.WaitGroup
	ctx context.Context

	ns       dto.NS
	url      string // source
	connMode string

	limit  int
	bucket *ratelimit.Bucket

	checkChan <-chan bson.M
}
