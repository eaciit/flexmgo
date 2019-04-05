package flexmgo

import (
	"context"
	"strings"
	"time"

	"git.eaciitapp.com/sebar/dbflex"
	"github.com/eaciit/toolkit"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Connection struct {
	dbflex.ConnectionBase `bson:"-" json:"-"`
	ctx                   context.Context
	client                *mongo.Client
	db                    *mongo.Database
}

func (c *Connection) Connect() error {
	connURI := "mongodb://"
	connURI += c.Host + "/"

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
		}
	}

	//toolkit.Logger().Debugf("opts: %s", toolkit.JsonString(opts))
	client, err := mongo.NewClient(opts)
	if err != nil {
		return err
	}

	//toolkit.Logger().Debug("client generated: OK")
	if c.ctx == nil {
		c.ctx = context.Background()
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

/*
func (c *Connection) Prepare(dbflex.ICommand) (dbflex.IQuery, error) {
	panic("not implemented")
}

func (c *Connection) Execute(dbflex.ICommand, toolkit.M) (interface{}, error) {
	panic("not implemented")
}

func (c *Connection) Cursor(dbflex.ICommand, toolkit.M) dbflex.ICursor {
	panic("not implemented")
}

func (c *Connection) NewQuery() dbflex.IQuery {
	panic("not implemented")
}

func (c *Connection) ObjectNames(dbflex.ObjTypeEnum) []string {
	panic("not implemented")
}

func (c *Connection) ValidateTable(interface{}, bool) error {
	panic("not implemented")
}

func (c *Connection) DropTable(string) error {
	panic("not implemented")
}

func (c *Connection) SetThis(dbflex.IConnection) dbflex.IConnection {
	panic("not implemented")
}

func (c *Connection) This() dbflex.IConnection {
	panic("not implemented")
}

func (c *Connection) SetFieldNameTag(string) {
	panic("not implemented")
}

func (c *Connection) FieldNameTag() string {
	panic("not implemented")
}
*/
