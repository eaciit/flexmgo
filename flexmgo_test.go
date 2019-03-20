package flexmgo_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"git.eaciitapp.com/sebar/dbflex/orm"
	"github.com/eaciit/flexmgo"
	_ "github.com/eaciit/flexmgo"

	"git.eaciitapp.com/sebar/dbflex"
	"github.com/eaciit/toolkit"
	cv "github.com/smartystreets/goconvey/convey"
)

const (
	connTxt = "mongodb://localhost:27017/dbapp"
)

func init() {
	toolkit.Logger().SetLevelStdOut(toolkit.DebugLevel, true)
}

func TestConnect(t *testing.T) {
	cv.Convey("reading connection", t, func() {
		conn, err := connect()
		cv.So(err, cv.ShouldBeNil)
		defer conn.Close()
	})
}

var tablename = "testrecord"

func TestConnectFail(t *testing.T) {
	cv.Convey("connect", t, func() {
		conn, err := dbflex.NewConnectionFromURI("mongodb://my-localhost:21234/db1", nil)
		err = conn.Connect()
		cv.So(err, cv.ShouldNotBeNil)
	})
}

func TestClassicRead(t *testing.T) {
	cv.Convey("connect", t, func() {
		conn, err := connect()
		cv.So(err, cv.ShouldBeNil)
		defer conn.Close()

		cv.Convey("read", func() {
			ctx := context.Background()
			db := conn.(*flexmgo.Connection).Mdb()
			cur, err := db.Collection("appusers").Find(ctx, toolkit.M{"_id": toolkit.M{"$eq": "user03"}})
			cv.So(err, cv.ShouldBeNil)
			defer cur.Close(ctx)

			cv.Convey("validate", func() {
				data := new(struct {
					ID    string `bson:"_id"`
					Name  string
					Email string
				})
				i := 0
				for {
					if cur.Next(ctx) {
						i++
						err := cur.Decode(data)
						cv.So(err, cv.ShouldBeNil)

						cv.So(data.ID, cv.ShouldEqual, "user03")
						toolkit.Logger().Debugf("data: %s", toolkit.JsonString(data))
					} else {
						break
					}
				}
				cv.So(i, cv.ShouldEqual, 1)
			})
		})
	})
}

func TestRunCommandClassic(t *testing.T) {
	//{"count":"testrecord","query":{"_id":{"$eq":"record-id-3"}}}
	cv.Convey("connect", t, func() {
		conn, err := connect()
		cv.So(err, cv.ShouldBeNil)
		defer conn.Close()

		cv.Convey("runcommand", func() {
			db := conn.(*flexmgo.Connection).Mdb()

			sr := db.RunCommand(nil, toolkit.M{}.Set("count", "testrecord"))
			cv.So(sr.Err(), cv.ShouldBeNil)

			countModel := new(struct {
				N int
			})

			err = sr.Decode(countModel)
			cv.So(err, cv.ShouldBeNil)
			cv.So(countModel.N, cv.ShouldEqual, 10)
		})
	})
}

func TestSaveData(t *testing.T) {
	cv.Convey("connect", t, func() {
		conn, err := connect()
		cv.So(err, cv.ShouldBeNil)
		defer conn.Close()

		cv.Convey("saving data", func() {
			cmd := dbflex.From(tablename).Save()
			es := []error{}
			for i := 1; i <= 10; i++ {
				r := new(Record)
				r.ID = toolkit.Sprintf("record-id-%d", i)
				r.Title = "Title is " + toolkit.RandomString(32)
				r.Age = toolkit.RandInt(10) + 18
				r.Salary = toolkit.RandFloat(8000, 4) + float64(5000)
				r.DateJoin = time.Date(2000, 1, 1, 0, 0, 0, 0,
					time.Now().Location()).Add(24 * time.Hour * time.Duration(toolkit.RandInt(1000)))

				if _, err := conn.Execute(cmd, toolkit.M{}.Set("data", r)); err != nil {
					es = append(es, err)
					break
				}
			}

			txts := []string{}
			for _, e := range es {
				txts = append(txts, e.Error())
			}

			cv.So(strings.Join(txts, "\n"), cv.ShouldEqual, "")
		})
	})
}

func TestUpdateData(t *testing.T) {
	cv.Convey("connect", t, func() {
		conn, err := connect()
		cv.So(err, cv.ShouldBeNil)
		defer conn.Close()

		cv.Convey("get data", func() {
			r := new(Record)
			cmdget := dbflex.From(tablename).Select().Where(dbflex.Eq("_id", "record-id-3"))
			cur := conn.Cursor(cmdget, nil)
			err := cur.Fetch(r)
			cv.So(err, cv.ShouldBeNil)
			cv.So(r.Title, cv.ShouldStartWith, "Title is ")

			cv.Convey("update data", func() {
				_, err = conn.Execute(dbflex.From(tablename).Save(), toolkit.M{}.Set("data", r))
				cv.So(err, cv.ShouldBeNil)

				cv.Convey("vaidate", func() {
					cur = conn.Cursor(cmdget, nil)
					count := cur.Count()
					cv.So(count, cv.ShouldEqual, 1)
				})
			})
		})
	})
}

func connect() (dbflex.IConnection, error) {
	if conn, err := dbflex.NewConnectionFromURI(connTxt, nil); err == nil {
		if err = conn.Connect(); err == nil {
			return conn, nil
		} else {
			return nil, err
		}
	} else {
		return nil, err
	}
	return nil, errors.New("not implemented yet")
}

type Record struct {
	orm.DataModelBase `bson:"-" json:"-" ecname:"-"`
	ID                string `bson:"_id" json:"_id" ecname:"_id"`
	Title             string `bson:"name" json:"name" ecname:"name"`
	Age               int
	Salary            float64
	DateJoin          time.Time
}

func (r *Record) TableName() string {
	return tablename
}

func (r *Record) GetID() ([]string, []interface{}) {
	return []string{"_id"}, []interface{}{r.ID}
}

func (r *Record) SetID(obj []interface{}) {
	r.ID = obj[0].(string)
}
