package flexmgo

import (
	"context"
	"fmt"
	"strings"
	"time"

	"git.kanosolution.net/kano/dbflex"
	"github.com/eaciit/toolkit"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readconcern"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"
)

type Connection struct {
	dbflex.ConnectionBase `bson:"-" json:"-"`
	ctx                   context.Context
	client                *mongo.Client
	db                    *mongo.Database
	sess                  mongo.Session
}

func (c *Connection) Connect() error {
	configString := "?"
	for k, v := range c.Config {
		configString += k + "=" + v.(string) + "&"
	}

	connURI := "mongodb://"
	connURI += c.Host + "/"
	connURI += configString

	opts := options.Client().ApplyURI(connURI)
	//opts.SetConnectTimeout(5 * time.Second)
	//opts.SetSocketTimeout(3 * time.Second)
	//opts.SetServerSelectionTimeout(3 * time.Second)
	if c.User != "" {
		opts.SetAuth(options.Credential{
			Username:   c.User,
			Password:   c.Password,
			AuthSource: "admin",
		})
	}

	for k, v := range c.Config {
		klow := strings.ToLower(k)
		switch klow {
		case "serverselectiontimeout":
			opts.SetServerSelectionTimeout(
				time.Duration(toolkit.ToInt(v, toolkit.RoundingAuto)) * time.Millisecond)

		case "replicaset":
			opts.SetReplicaSet(v.(string))
			//opts.SetWriteConcern()

		case "poolsize":
			poolSize := toolkit.ToInt(v.(string), toolkit.RoundingAuto)
			if poolSize > 0 {
				opts.SetMaxPoolSize(uint64(poolSize))
			}

		case "idle":
			idle := toolkit.ToInt(v.(string), toolkit.RoundingAuto)
			if idle > 0 {
				opts.SetMaxConnIdleTime(time.Duration(idle) * time.Second)
			}
		}
	}

	//toolkit.Logger().Debugf("opts: %s", toolkit.JsonString(opts))
	client, err := mongo.NewClient(opts)
	if err != nil {
		return err
	}

	//toolkit.Logger().Debug("client generated: OK")
	if c.ctx == nil {
		c.ctx = context.TODO()
	}

	//toolkit.Logger().Debug("context generated: OK")
	if err = client.Connect(c.ctx); err != nil {
		return err
	}

	//toolkit.Logger().Debug("client connected: OK")
	if err = client.Ping(c.ctx, nil); err != nil {
		return err
	}

	c.client = client
	if c.Database != "" {
		c.db = c.client.Database(c.Database)
	}

	return nil
}

func (c *Connection) Mdb() *mongo.Database {
	return c.db
}

func (c *Connection) State() string {
	if c.client == nil {
		return dbflex.StateUnknown
	} else {
		return dbflex.StateConnected
	}
}

func (c *Connection) Close() {
	if c.client != nil {
		c.client.Disconnect(c.ctx)
		c.client = nil
	}
}

func (c *Connection) NewQuery() dbflex.IQuery {
	q := new(Query)
	q.SetThis(q)
	q.SetConnection(c)

	return q
}

func (c *Connection) DropTable(name string) error {
	return c.db.Collection(name).Drop(c.ctx)
}

func (c *Connection) BeginTx() error {
	wc := writeconcern.New(writeconcern.WMajority())
	rc := readconcern.Snapshot()
	txnOpts := options.Transaction().SetWriteConcern(wc).SetReadConcern(rc)

	if c.sess != nil {
		return fmt.Errorf("session already exist. Pls commit or rollback last")
	}

	sess, err := c.client.StartSession()
	if err != nil {
		return fmt.Errorf("unable to start new transaction. %s", err.Error())
	}
	sess.StartTransaction(txnOpts)
	c.sess = sess
	return nil
}

func (c *Connection) Commit() error {
	if c.sess == nil {
		return fmt.Errorf("transaction session is not exists yet")
	}

	err := c.sess.CommitTransaction(c.ctx)
	if err != nil {
		return fmt.Errorf("unable to commit. %s", err.Error())
	}

	c.sess = nil
	return nil
}

func (c *Connection) RollBack() error {
	if c.sess == nil {
		return fmt.Errorf("transaction session is not exists yet")
	}

	err := c.sess.AbortTransaction(c.ctx)
	if err != nil {
		return fmt.Errorf("unable to rollback. %s", err.Error())
	}

	c.sess = nil
	return nil
}

func (c *Connection) IsTx() bool {
	return c.sess != nil
}

// SupportTx to identify if underlying connection support Tx or not
func (c *Connection) SupportTx() bool {
	return true
}
