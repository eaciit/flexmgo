package flexmgo_test

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"git.kanosolution.net/kano/dbflex/orm"
	_ "github.com/ariefdarmawan/flexmgo"
	logger "github.com/sebarcode/logger"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"git.kanosolution.net/kano/dbflex"
	"github.com/sebarcode/codekit"
	"github.com/smartystreets/goconvey/convey"
	cv "github.com/smartystreets/goconvey/convey"
)

const (
	connTxt = "mongodb://localhost:27017/dbapp"
	host    = "mongodb://localhost:27017"
	dbname  = "dbapp"
)

func init() {
	fmt.Println("Debug level is activated")
	logger.Logger().SetLevelStdOut(logger.DebugLevel, true)
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
				r.ID = fmt.Sprintf("record-id-%d", i)
				r.Title = "Title is " + codekit.RandomString(32)
				r.Age = codekit.RandInt(10) + 18
				r.Salary = codekit.RandFloat(8000, 4) + float64(5000)
				r.DateJoin = time.Date(2000, 1, 1, 0, 0, 0, 0,
					time.Now().Location()).Add(24 * time.Hour * time.Duration(codekit.RandInt(1000)))

				if _, err := conn.Execute(cmd, codekit.M{}.Set("data", r)); err != nil {
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
			dbflex.Gt("Salary", 1000),
			func(r *Record) bool {
				return r.Salary > 1000
			},
		},

		"gte": {
			dbflex.Gte("Salary", 1000), func(r *Record) bool {
				return r.Salary >= 1000
			},
		},

		"lt": {dbflex.Lt("Age", 80), func(r *Record) bool {
			return r.Age < 80
		},
		},
		"lte": {dbflex.Lte("Age", 90), func(r *Record) bool {
			return r.Age <= 90
		},
		},
		"range": {dbflex.Range("DateJoin",
			codekit.String2Date("2000-01-01", "YYYY-MM-DD"),
			time.Now()), func(r *Record) bool {
			return r.DateJoin.After(codekit.String2Date("2000-01-01", "YYYY-MM-DD")) &&
				r.DateJoin.Before(time.Now())
		},
		},
	}

	for key, sc := range scenarios {
		if key != "range" {
			continue
		}
		cv.Convey("scenario "+key, t, func() {
			conn, err := connect()
			cv.So(err, cv.ShouldBeNil)
			defer conn.Close()

			cv.Convey("fetch ", func() {
				cur := conn.Cursor(dbflex.From(tablename).Select().Where(sc.filter), nil)
				defer cur.Close()
				rs := []*Record{}
				err := cur.Fetchs(&rs, 0).Error()
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
			err := cur.Fetch(r).Error()
			cv.So(err, cv.ShouldBeNil)
			cv.So(r.Title, cv.ShouldStartWith, "Title is ")

			cv.Convey("update data", func() {
				cmd := dbflex.From(tablename).Update("title")
				_, err = conn.Execute(cmd, codekit.M{}.Set("data", r))
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

type country struct {
	ID    string `bson:"_id" json:"_id" ecname:"_id"`
	Title string
}

type state struct {
	ID        string `bson:"_id" json:"_id" ecname:"_id"`
	Title     string
	CountryID string
}

var (
	countriesTableName = "countries"
	stateTableName     = "states"
)

func TestMdbTrx(t *testing.T) {
	cv.Convey("connect", t, func() {
		conn, err := connectTrx()
		cv.So(err, cv.ShouldBeNil)
		defer conn.Close()

		cv.Convey("insert countries without trx", func() {
			countries := []*country{
				{"SG", "Singapore"},
				{"ID", "Indonesia"},
				{"MY", "Malaysia"},
				{"IN", "India"},
			}

			err = nil
			cmd := dbflex.From(countriesTableName).Save()
			for _, country := range countries {
				_, err = conn.Execute(cmd, codekit.M{}.Set("data", country))
				if err != nil {
					break
				}
			}
			cv.So(err, cv.ShouldBeNil)

			cmd = dbflex.From(countriesTableName).Select()
			cur := conn.Cursor(cmd, nil)
			cv.So(cur.Error(), cv.ShouldBeNil)
			defer cur.Close()

			ms := []codekit.M{}
			cv.So(cur.Fetchs(&ms, 0).Error(), cv.ShouldBeNil)
			cv.So(len(ms), cv.ShouldEqual, len(countries))

			cv.Convey("insert state with trx", func() {
				conn.Execute(dbflex.From(stateTableName).Delete(), nil)

				err = conn.BeginTx()
				cv.So(err, cv.ShouldBeNil)

				states := []*state{
					{"SG", "Singapore", "SG"},
					{"JK", "Jakarta", "ID"},
					{"MB", "Mumbai", "IN"},
				}
				cmd := dbflex.From(stateTableName).Save()
				for _, state := range states {
					_, err = conn.Execute(cmd, codekit.M{}.Set("data", state))
					if err != nil {
						break
					}
				}
				cv.So(err, cv.ShouldBeNil)

				commitErr := conn.Commit()
				cv.So(commitErr, cv.ShouldBeNil)

				cmd = dbflex.From(stateTableName).Select()
				cur := conn.Cursor(cmd, nil)
				cv.So(cur.Error(), cv.ShouldBeNil)
				cur.Close()
				ms := []codekit.M{}
				cv.So(cur.Fetchs(&ms, 0).Error(), cv.ShouldBeNil)
				cv.So(len(ms), cv.ShouldEqual, len(states))

				cv.Convey("rollback", func() {
					err = conn.BeginTx()
					cv.So(err, cv.ShouldBeNil)

					cmd := dbflex.From(stateTableName).Insert()
					conn.Execute(cmd, codekit.M{}.Set("data", &state{"JT", "Jawa Timur", "ID"}))
					err = conn.RollBack()
					cv.So(err, cv.ShouldBeNil)

					cmd = dbflex.From(stateTableName).Select()
					cur := conn.Cursor(cmd, nil)
					cv.So(cur.Error(), cv.ShouldBeNil)
					cur.Close()
					ms1 := []codekit.M{}
					cv.So(cur.Fetchs(&ms1, 0).Error(), cv.ShouldBeNil)
					cv.So(len(ms1), cv.ShouldEqual, len(states))
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

		changed := make(chan codekit.M)
		cmd := dbflex.From(tablename).Command("watch")
		_, err = conn.Execute(cmd, codekit.M{}.
			Set("fn", func(data codekit.M) {
				changed <- data
				close(changed)
			}))
		cv.So(err, cv.ShouldBeNil)

		cv.Convey("validate", func() {
			m := codekit.M{}
			cmdGet := dbflex.From(tablename).Select().Where(dbflex.Eq("_id", "record-id-5"))
			err := conn.Cursor(cmdGet, nil).SetCloseAfterFetch().Fetch(&m)
			cv.So(err, cv.ShouldBeNil)

			m.Set("title", "Test Change Stream")
			_, err = conn.Execute(dbflex.From(tablename).Save(), codekit.M{}.Set("data", m))

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
			cmd := dbflex.From(tablename).Aggr(dbflex.NewAggrItem("Salary", dbflex.AggrSum, "Salary"))
			cur := conn.Cursor(cmd, nil)
			cv.So(cur.Error(), cv.ShouldBeNil)
			defer cur.Close()

			cv.Convey("validate", func() {
				cur2 := conn.Cursor(dbflex.From(tablename).Select(), nil)
				defer cur2.Close()

				total := float64(0)
				for {
					r := new(Record)
					if err := cur2.Fetch(r).Error(); err == nil {
						total += r.Salary
					} else {
						break
					}
				}

				aggrModel := new(struct{ Salary float64 })
				cur.Fetch(aggrModel)

				cv.So(math.Abs(aggrModel.Salary-total), cv.ShouldBeLessThan, 1)
				logger.Logger().Debugf("total is: %v", total)
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
		data := []byte(codekit.RandomString(512))
		conn, _ := connect()
		defer conn.Close()

		cv.Convey("writing to grid", func() {
			buff := bytes.NewReader([]byte(data))
			reader := bufio.NewReader(buff)

			cmd := dbflex.From("fs").Command("GfsWrite", nil)
			metadata := codekit.M{}.Set("data", "ini adalah meta")
			_, err := conn.Execute(cmd, codekit.M{}.
				Set("id", "doc1").
				Set("metadata", metadata).
				Set("source", reader))
			cv.So(err, cv.ShouldBeNil)

			cv.Convey("reading from grid", func() {
				var buff bytes.Buffer
				writer := bufio.NewWriter(&buff)

				cmd := dbflex.From("fs").Command("gfsread", nil)
				_, err := conn.Execute(cmd, codekit.M{}.
					Set("id", "doc1").
					Set("output", writer))
				cv.So(err, cv.ShouldBeNil)
				cv.So(string(data), cv.ShouldEqual, string(buff.Bytes()))

				cv.Convey("delete grid", func() {
					cmd := dbflex.From("fs").Command("gfsdelete", nil)
					_, err := conn.Execute(cmd, codekit.M{}.Set("id", "doc1"))
					cv.So(err, cv.ShouldBeNil)
				})
			})
		})
	})
}

func TestCheckIndex(t *testing.T) {
	cv.Convey("connect and create index", t, func() {
		conn, _ := connect()
		defer conn.Close()

		e := conn.EnsureIndex(tablename, "record_salary_index", false, "Age", "-Salary")
		cv.So(e, cv.ShouldBeNil)

		cv.Convey("validate", func() {
			ctx := context.TODO()
			mconn, _ := mongo.Connect(ctx, options.Client().ApplyURI(host))
			defer mconn.Disconnect(ctx)

			indexCursor, e := mconn.Database(dbname).Collection(tablename).Indexes().List(ctx)
			cv.So(e, cv.ShouldBeNil)

			indexModels := []bson.M{}
			indexCursor.All(ctx, &indexModels)

			found := false
			indexOk := false
			for _, index := range indexModels {
				//cv.Println(codekit.JsonString(index))
				name, nameOk := index["name"].(string)
				if nameOk && name == "record_salary_index" {
					found = true
					key := index["key"].(primitive.M)
					indexOk = key["Age"].(int32) == 1 && key["Salary"].(int32) == -1
				}
			}

			cv.So(found, cv.ShouldBeTrue)
			cv.So(indexOk, cv.ShouldBeTrue)

			convey.Convey("reindex", func() {
				e := conn.EnsureIndex(tablename, "record_salary_index", false, "Age", "-Salary")
				cv.So(e, cv.ShouldBeNil)

				mconn.Database(dbname).Collection(tablename).Indexes().DropOne(ctx, "record_salary_index")
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
			conn.SetFieldNameTag("json")
			return conn, nil
		} else {
			return nil, err
		}
	} else {
		return nil, err
	}
}

func connectTrx() (dbflex.IConnection, error) {
	if conn, err := dbflex.NewConnectionFromURI("mongodb://localhost:27017/rsdb", nil); err == nil {
		if err = conn.Connect(); err == nil {
			conn.SetFieldNameTag("json")
			return conn, nil
		} else {
			return nil, err
		}
	} else {
		return nil, err
	}
}

type Record struct {
	orm.DataModelBase `bson:"-" json:"-"`
	ID                string `bson:"_id" json:"_id"`
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

/*
var demoConfig = {
    _id: "rs",
    members: [
        { _id: 0,
          host: 'localhost:27017',
          priority: 10
        },
        { _id: 1,
          host: 'localhost:27018'
        }
    ]
 };
*/
