package flexmgo

import (
	"fmt"
	"io"
	"reflect"
	"time"

	"git.kanosolution.net/kano/dbflex"
	"github.com/ariefdarmawan/serde"
	"github.com/sebarcode/codekit"
	"go.mongodb.org/mongo-driver/mongo"
)

type Cursor struct {
	dbflex.CursorBase
	mc *mongo.Cursor

	tablename string
	countParm codekit.M
	conn      *Connection
	cursor    *mongo.Cursor
}

func (cr *Cursor) Close() error {
	e := cr.Error()
	if cr.mc != nil {
		cr.mc.Close(cr.conn.ctx)
	}
	return e
}

func (cr *Cursor) Count() int {
	if cr.countParm == nil || len(cr.countParm) == 0 {
		return 0
	}

	if cr.countParm.Get("count") == "" {
		return 0
	}

	tableName := cr.countParm.GetString("count")
	where := cr.countParm.Get("query", nil)
	if where == nil {
		n, _ := cr.conn.db.Collection(tableName).CountDocuments(cr.conn.ctx, codekit.M{})
		return int(n)
	} else {
		n, _ := cr.conn.db.Collection(tableName).CountDocuments(cr.conn.ctx, where)
		return int(n)
	}

	/*
		sr := cr.conn.db.RunCommand(cr.conn.ctx, cr.countParm)
		if sr.Err() != nil {
			dbflex.Logger().Errorf("unable to get count. %s, countparm: %s",
				sr.Err().Error(),
				codekit.JsonString(cr.countParm))
			return 0
		}

		countModel := new(struct{ N int })
		if err := sr.Decode(countModel); err != nil {
			dbflex.Logger().Errorf("unablet to decode count. %s", sr.Err().Error())
			return 0
		}
		return countModel.N
	*/
}

func (cr *Cursor) Fetch(out interface{}) dbflex.ICursor {
	if cr.Error() != nil {
		cr.SetError(fmt.Errorf("unable to fetch data. %s", cr.Error()))
		return cr
	}

	if neof := cr.cursor.Next(cr.conn.ctx); !neof {
		cr.SetError(io.EOF)
		return cr
	}

	m := codekit.M{}
	if err := cr.cursor.Decode(&m); err != nil {
		cr.SetError(fmt.Errorf("unable to decode output. %s", err.Error()))
		return cr
	}
	for mk, mv := range m {
		// update date value to date
		if mvs, ok := mv.(string); ok && len(mvs) >= 11 {
			if mvs[4] == '-' && mvs[7] == '-' && mvs[10] == 'T' {
				if dt, err := time.Parse(time.RFC3339, mvs); err == nil {
					m.Set(mk, dt)
				}
			}
		}
	}
	if reflect.ValueOf(m).Type().String() == reflect.Indirect(reflect.ValueOf(out)).Type().String() {
		reflect.ValueOf(out).Elem().Set(reflect.ValueOf(m))
	} else {
		if err := serde.Serde(m, out); err != nil {
			cr.SetError(fmt.Errorf("unable to decode output. %s", err.Error()))
			return cr
		}
	}

	return cr
}

func (cr *Cursor) Fetchs(result interface{}, n int) dbflex.ICursor {
	if cr.Error() != nil {
		cr.SetError(fmt.Errorf("unable to fetch data. %s", cr.Error()))
		return cr
	}

	/*
		v := reflect.TypeOf(result).Elem().Elem()
		ivs := reflect.MakeSlice(reflect.SliceOf(v), 0, 0)

			read := 0
			for {
				if !cr.cursor.Next(cr.conn.ctx) {
					break
				}

				iv := reflect.New(v).Interface()
				err := cr.cursor.Decode(iv)
				if err != nil {
					cr.SetError(fmt.Errorf("unable to decode cursor data. %s", err.Error()))
					return cr
				}
				ivs = reflect.Append(ivs, reflect.ValueOf(iv).Elem())

				read++
				if n != 0 && read == n {
					break
				}
			}
			reflect.ValueOf(result).Elem().Set(ivs)
	*/
	read := 0
	ms := []codekit.M{}
	for {
		if !cr.cursor.Next(cr.conn.ctx) {
			break
		}

		m := codekit.M{}
		err := cr.cursor.Decode(&m)
		if err != nil {
			cr.SetError(fmt.Errorf("unable to decode cursor data. %s", err.Error()))
			return cr
		}
		for mk, mv := range m {
			// update date value to date
			if mvs, ok := mv.(string); ok && len(mvs) >= 11 {
				if mvs[4] == '-' && mvs[7] == '-' && mvs[10] == 'T' {
					if dt, err := time.Parse(time.RFC3339, mvs); err == nil {
						m.Set(mk, dt)
						//fmt.Println(mk, mvs, dt, m, fmt.Sprintf("%t", m.Get("Created")))
					}
				}
			}
		}
		ms = append(ms, m)

		read++
		if n != 0 && read == n {
			break
		}
	}
	if reflect.ValueOf(ms).Type().String() == reflect.Indirect(reflect.ValueOf(result)).Type().String() {
		reflect.ValueOf(result).Elem().Set(reflect.ValueOf(ms))
	} else {
		if err := serde.Serde(ms, result); err != nil {
			cr.SetError(fmt.Errorf("unable to decode cursor data. %s", err.Error()))
			return cr
		}
	}

	return cr
}

/*
func (cr *Cursor) Reset() error {
	panic("not implemented")
}

func (cr *Cursor) Fetch(interface{}) error {
	panic("not implemented")
}

func (cr *Cursor) Fetchs(interface{}, int) error {
	panic("not implemented")
}


func (cr *Cursor) CountAsync() <-chan int {
	panic("not implemented")
}

func (cr *Cursor) Error() error {
	panic("not implemented")
}

func (cr *Cursor) CloseAfterFetch() bool {
	panic("not implemented")
}

func (cr *Cursor) SetCountCommand(dbflex.ICommand) {
	panic("not implemented")
}

func (cr *Cursor) CountCommand() dbflex.ICommand {
	panic("not implemented")
}

func (cr *Cursor) Connection() dbflex.IConnection {
	panic("not implemented")
}

func (cr *Cursor) SetConnection(dbflex.IConnection) {
	panic("not implemented")
}

func (cr *Cursor) ConfigRef(key string, def interface{}, out interface{}) {
	panic("not implemented")
}

func (cr *Cursor) Set(key string, value interface{}) {
	panic("not implemented")
}

func (cr *Cursor) SetCloseAfterFetch() dbflex.ICursor {
	panic("not implemented")
}

func (cr *Cursor) AutoClose(time.Duration) dbflex.ICursor {
	panic("not implemented")
}
*/
