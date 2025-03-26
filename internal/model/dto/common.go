package dto

import (
	"fmt"
	"strings"
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
