package flexmgo

import (
	"fmt"
	"io"
	"reflect"

	"git.kanosolution.net/kano/dbflex"
	"github.com/eaciit/toolkit"
	"go.mongodb.org/mongo-driver/mongo"
)

type Cursor struct {
	dbflex.CursorBase
	mc *mongo.Cursor

	tablename string
	countParm toolkit.M
	conn      *Connection
	cursor    *mongo.Cursor
}

func (cr *Cursor) Close() {
	if cr.mc != nil {
		cr.mc.Close(cr.conn.ctx)
	}
}

func (cr *Cursor) Count() int {
	if cr.countParm == nil || len(cr.countParm) == 0 {
		return 0
	}

	if cr.countParm.Get("count") == "" {
		return 0
	}

	sr := cr.conn.db.RunCommand(cr.conn.ctx, cr.countParm)
	if sr.Err() != nil {
		dbflex.Logger().Errorf("unable to get count. %s, countparm: %s",
			sr.Err().Error(),
			toolkit.JsonString(cr.countParm))
		return 0
	}

	countModel := new(struct{ N int })
	if err := sr.Decode(countModel); err != nil {
		dbflex.Logger().Errorf("unablet to decode count. %s", sr.Err().Error())
		return 0
	}
	return countModel.N
}

func (cr *Cursor) Fetch(out interface{}) error {
	if cr.Error() != nil {
		return toolkit.Errorf("unable to fetch data. %s", cr.Error())
	}

	if neof := cr.cursor.Next(cr.conn.ctx); !neof {
		return io.EOF
	}

	if err := cr.cursor.Decode(out); err != nil {
		return toolkit.Errorf("unable to decode output. %s", err.Error())
	}

	return nil
}

func (cr *Cursor) Fetchs(result interface{}, n int) error {
	if cr.Error() != nil {
		return toolkit.Errorf("unable to fetch data. %s", cr.Error())
	}

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
			return fmt.Errorf("unable to decode cursor data. %s", err.Error())
		}
		ivs = reflect.Append(ivs, reflect.ValueOf(iv).Elem())

		read++
		if n != 0 && read == n {
			break
		}
	}
	reflect.ValueOf(result).Elem().Set(ivs)
	return nil
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
