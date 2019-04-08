package flexmgo_test

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"git.eaciitapp.com/sebar/dbflex/orm"
	_ "github.com/eaciit/flexmgo"

	"git.eaciitapp.com/sebar/dbflex"
	"github.com/eaciit/toolkit"
	cv "github.com/smartystreets/goconvey/convey"
)

const (
	connTxt = "mongodb://localhost:27017/dbapp"
)

func init() {
	fmt.Println("Debug level is activated")
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
		conn, err := dbflex.NewConnectionFromURI("mongodb://my-localhost:21234/db1?serverSelectionTimeout=1000", nil)
		err = conn.Connect()
		cv.So(err, cv.ShouldNotBeNil)
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

func TestListData(t *testing.T) {
	scenarios := map[string]struct {
		filter    *dbflex.Filter
		validator func(*Record) bool
	}{
		"eq": {
			dbflex.Eq("_id", "record-id-8"),
			func(r *Record) bool {
				return r.ID == "record-id-8"
			},
		},

		"ne": {
			dbflex.Ne("_id", "record-id-8"),
			func(r *Record) bool {
				return r.ID != "record-id-8"
			},
		},

		"gt": {
			dbflex.Gt("salary", 1000),
			func(r *Record) bool {
				return r.Salary > 1000
			},
		},

		"gte": {
			dbflex.Gte("salary", 1000), func(r *Record) bool {
				return r.Salary >= 1000
			},
		},

		"lt": {dbflex.Lt("age", 80), func(r *Record) bool {
			return r.Age < 80
		},
		},
		"lte": {dbflex.Lte("age", 90), func(r *Record) bool {
			return r.Age <= 90
		},
		},
		"range": {dbflex.Range("datejoin",
			toolkit.String2Date("2000-01-01", "YYYY-mm-dd"),
			time.Now()), func(r *Record) bool {
			return r.DateJoin.After(toolkit.String2Date("2000-01-01", "YYYY-mm-dd")) &&
				r.DateJoin.Before(time.Now())
		},
		},
	}

	for key, sc := range scenarios {
		cv.Convey("scenario "+key, t, func() {
			conn, err := connect()
			cv.So(err, cv.ShouldBeNil)
			defer conn.Close()

			cv.Convey("fetchs", func() {
				cur := conn.Cursor(dbflex.From(tablename).Select().Where(sc.filter), nil)
				defer cur.Close()
				rs := []*Record{}
				err := cur.Fetchs(&rs, 0)
				cv.So(err, cv.ShouldBeNil)
				cv.So(len(rs), cv.ShouldBeGreaterThan, 0)
				cv.So(sc.validator(rs[0]), cv.ShouldBeTrue)
			})
		})
	}
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

/*
func TestWatch(t *testing.T) {
	cv.Convey("change stream", t, func() {
		conn, err := connect()
		cv.So(err, cv.ShouldBeNil)
		defer conn.Close()

		changed := make(chan toolkit.M)
		cmd := dbflex.From(tablename).Command("watch")
		_, err = conn.Execute(cmd, toolkit.M{}.
			Set("fn", func(data toolkit.M) {
				changed <- data
				close(changed)
			}))
		cv.So(err, cv.ShouldBeNil)

		cv.Convey("validate", func() {
			m := toolkit.M{}
			cmdGet := dbflex.From(tablename).Select().Where(dbflex.Eq("_id", "record-id-5"))
			err := conn.Cursor(cmdGet, nil).SetCloseAfterFetch().Fetch(&m)
			cv.So(err, cv.ShouldBeNil)

			m.Set("title", "Test Change Stream")
			_, err = conn.Execute(dbflex.From(tablename).Save(), toolkit.M{}.Set("data", m))

			changedData := <-changed
			cv.So(changedData.GetString("title"), cv.ShouldEqual, "Test Change Stream")
		})
	})
}
*/

func TestDeleteData(t *testing.T) {
	cv.Convey("connect", t, func() {
		conn, err := connect()
		cv.So(err, cv.ShouldBeNil)
		defer conn.Close()

		cv.Convey("delete record-5", func() {
			_, err := conn.Execute(dbflex.From(tablename).
				Where(dbflex.Eq("_id", "record-id-5")).Delete(), nil)
			cv.So(err, cv.ShouldBeNil)

			cv.Convey("validate", func() {
				cur := conn.Cursor(dbflex.From(tablename).Select(), nil)
				defer cur.Close()

				cv.So(cur.Count(), cv.ShouldEqual, 9)
			})
		})
	})
}

func TestAggregateData(t *testing.T) {
	cv.Convey("connect", t, func() {
		conn, err := connect()
		cv.So(err, cv.ShouldBeNil)
		defer conn.Close()

		cv.Convey("aggregate", func() {
			cmd := dbflex.From(tablename).Aggr(dbflex.NewAggrItem("salary", dbflex.AggrSum, "salary"))
			cur := conn.Cursor(cmd, nil)
			cv.So(cur.Error(), cv.ShouldBeNil)
			defer cur.Close()

			cv.Convey("validate", func() {
				cur2 := conn.Cursor(dbflex.From(tablename).Select(), nil)
				defer cur2.Close()

				total := float64(0)
				for {
					r := new(Record)
					if err := cur2.Fetch(r); err == nil {
						total += r.Salary
					} else {
						break
					}
				}

				aggrModel := new(struct{ Salary float64 })
				cur.Fetch(aggrModel)

				cv.So(math.Abs(aggrModel.Salary-total), cv.ShouldBeLessThan, 1)
				toolkit.Logger().Debugf("total is: %v", total)
			})
		})
	})
}

func TestDropTable(t *testing.T) {
	cv.Convey("connect", t, func() {
		conn, err := connect()
		cv.So(err, cv.ShouldBeNil)
		defer conn.Close()

		cv.Convey("drop table", func() {
			err := conn.DropTable(tablename)
			cv.So(err, cv.ShouldBeNil)
		})
	})
}

func TestGridFsUpdate(t *testing.T) {
	cv.Convey("preparing file", t, func() {
		data := []byte(toolkit.RandomString(512))
		conn, _ := connect()
		defer conn.Close()

		cv.Convey("writing to grid", func() {
			buff := bytes.NewReader([]byte(data))
			reader := bufio.NewReader(buff)

			cmd := dbflex.From("fs").Command("GfsWrite")
			metadata := toolkit.M{}.Set("data", "ini adalah meta")
			_, err := conn.Execute(cmd, toolkit.M{}.
				Set("id", "doc1").
				Set("metadata", metadata).
				Set("source", reader))
			cv.So(err, cv.ShouldBeNil)

			cv.Convey("reading from grid", func() {
				var buff bytes.Buffer
				writer := bufio.NewWriter(&buff)

				cmd := dbflex.From("fs").Command("gfsread")
				_, err := conn.Execute(cmd, toolkit.M{}.
					Set("id", "doc1").
					Set("output", writer))
				cv.So(err, cv.ShouldBeNil)
				cv.So(string(data), cv.ShouldEqual, string(buff.Bytes()))

				cv.Convey("delete grid", func() {
					cmd := dbflex.From("fs").Command("gfsdelete")
					_, err := conn.Execute(cmd, toolkit.M{}.Set("id", "doc1"))
					cv.So(err, cv.ShouldBeNil)
				})
			})
		})
	})
}

/*
TO DO
- Command Cursor
- Command Exec
- ChangeStream
*/

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
	Title             string
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
