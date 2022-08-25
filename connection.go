package flexmgo

import (
	"context"
	"fmt"
	"strings"
	"time"

	"git.kanosolution.net/kano/dbflex"
	"github.com/sebarcode/codekit"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
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

	_disableTx bool
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
				time.Duration(codekit.ToInt(v, codekit.RoundingAuto)) * time.Millisecond)

		case "replicaset":
			opts.SetReplicaSet(v.(string))
			//opts.SetWriteConcern()

		case "poolsize":
			poolSize := codekit.ToInt(v.(string), codekit.RoundingAuto)
			if poolSize > 0 {
				opts.SetMaxPoolSize(uint64(poolSize))
			}

		case "idle":
			idle := codekit.ToInt(v.(string), codekit.RoundingAuto)
			if idle > 0 {
				opts.SetMaxConnIdleTime(time.Duration(idle) * time.Second)
			}
		}
	}

	//logger.Logger().Debugf("opts: %s", codekit.JsonString(opts))
	client, err := mongo.NewClient(opts)
	if err != nil {
		return err
	}

	//logger.Logger().Debug("client generated: OK")
	if c.ctx == nil {
		c.ctx = context.TODO()
	}

	//logger.Logger().Debug("context generated: OK")
	if err = client.Connect(c.ctx); err != nil {
		return err
	}

	//logger.Logger().Debug("client connected: OK")
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

func (c *Connection) EnsureTable(name string, keys []string, obj interface{}) error {
	return nil
}

func (c *Connection) EnsureIndex(tableName, indexName string, isUnique bool, fields ...string) error {
	indexFound := false
	currentIndex := bson.M{}
	ctx := context.Background()
	coll := c.db.Collection(tableName)
	cursorIndex, e := coll.Indexes().List(ctx)
	if e != nil {
		return e
	}

	for cursorIndex.Next(ctx) {
		if e := cursorIndex.Decode(&currentIndex); e != nil {
			continue
		}

		if currentIndex["name"].(string) == indexName {
			indexFound = true
			break
		}
	}

	createIndex := false
	if indexFound {
		keys := currentIndex["key"].(primitive.M)
		unique, uniqueOK := currentIndex["unique"]
		if uniqueOK && unique != isUnique {
			createIndex = true
		} else {
		keyChecking:
			for _, f := range fields {
				fieldName := f
				indexValue := 1
				if f[0] == '-' {
					fieldName = f[1:]
					indexValue = -1
				}

				if existingIndexValue, ok := keys[fieldName]; !ok {
					createIndex = true
					break keyChecking
				} else if existingIndexValue != indexValue {
					createIndex = true
					break keyChecking
				}
			}

		}
	} else {
		createIndex = true
	}

	if createIndex {
		if indexFound {
			coll.Indexes().DropOne(ctx, indexName)
		}

		indexKeys := bson.D{}
		for _, f := range fields {
			if f[0] == '-' {
				indexKeys = append(indexKeys, bson.E{f[1:], -1})
			} else {
				indexKeys = append(indexKeys, bson.E{f, 1})
			}
		}

		if _, err := coll.Indexes().CreateOne(ctx, mongo.IndexModel{
			Keys:    indexKeys,
			Options: options.Index().SetName(indexName).SetUnique(isUnique),
		}); err != nil {
			return err
		}

	}
	return nil
}

func (c *Connection) DropTable(name string) error {
	return c.db.Collection(name).Drop(c.ctx)
}

func (c *Connection) BeginTx() error {
	if c._disableTx {
		return errors.New("tx is disabled")
	}

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

func (c *Connection) DisableTx() {
	c._disableTx = true
}

func (c *Connection) IsTx() bool {
	return c.sess != nil
}

// SupportTx to identify if underlying connection support Tx or not
func (c *Connection) SupportTx() bool {
	if (c._disableTx) {
		return false
	}
	return true
}
