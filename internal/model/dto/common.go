package dto

import (
	"fmt"
	"strings"

	"go.mongodb.org/mongo-driver/bson"
)

type NS struct {
	Database   string
	Collection string
}

func (ns NS) Str() string {
	return fmt.Sprintf("%s.%s", ns.Database, ns.Collection)
}

func NewNS(namespace string) NS {
	pair := strings.SplitN(namespace, ".", 2)
	return NS{Database: pair[0], Collection: pair[1]}
}

type CheckMsg struct {
	Id   any
	Raw  bson.Raw
	Done bool
}

type Metric struct {
	Ns        NS
	Total     int64
	Processed int64
	WrongNum  int64
	NotFound  int64
}

type CheckRes string

const (
	Equal    CheckRes = "equal"
	Wrong    CheckRes = "wrong"
	NotFound CheckRes = "not_found"
	Error    CheckRes = "error"
)

const (
	_ = iota
	First
	Second
)
