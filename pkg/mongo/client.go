package mg

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"

	l "mongo-checker/pkg/log"
	"mongo-checker/utils"
	"mongo-checker/vars"
)

type Conn struct {
	URL    string
	Client *mongo.Client
}

func NewMongoConn(url string, connectMode string) (*Conn, error) {
	clientOps := options.Client().ApplyURI(url)

	// read pref
	readPreference := readpref.Primary()
	switch connectMode {
	case vars.ConnectModePrimary:
		readPreference = readpref.Primary()
	case vars.ConnectModeSecondaryPreferred:
		readPreference = readpref.SecondaryPreferred()
	case vars.ConnectModeStandalone:
		// TODO, no standalone, choose nearest
		fallthrough
	case vars.ConnectModeNearset:
		readPreference = readpref.Nearest()
	default:
		readPreference = readpref.Primary()
	}
	clientOps.SetReadPreference(readPreference)

	// create default context
	ctx := context.Background()

	// connect
	client, err := mongo.NewClient(clientOps)
	if err != nil {
		return nil, fmt.Errorf("new client failed: %v", err)
	}
	if err = client.Connect(ctx); err != nil {
		return nil, fmt.Errorf("connect to %s failed: %v", utils.BlockPassword(url, "***"), err)
	}

	// ping
	if err = client.Ping(ctx, clientOps.ReadPreference); err != nil {
		return nil, fmt.Errorf("ping to %v failed: %v\n"+
			"If Mongo Server is standalone(single node) Or conn address is different with mongo server address"+
			" try atandalone mode by mongodb://ip:port/admin?connect=direct",
			utils.BlockPassword(url, "***"), err)
	}

	l.Logger.Infof("New session to %s successfully", utils.BlockPassword(url, "***"))
	return &Conn{
		URL:    url,
		Client: client,
	}, nil
}

func (c *Conn) Close() error {
	l.Logger.Infof("Close client with %s", utils.BlockPassword(c.URL, "***"))
	return c.Client.Disconnect(context.Background())
}

func (c *Conn) IsGood() bool {
	if err := c.Client.Ping(nil, nil); err != nil {
		return false
	}

	return true
}
