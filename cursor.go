package flexmgo

import (
	"fmt"
	"io"
	"reflect"

	"git.kanosolution.net/kano/dbflex"
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

	if err := cr.cursor.Decode(out); err != nil {
		cr.SetError(fmt.Errorf("unable to decode output. %s", err.Error()))
		return cr
	}

	return cr
}

func (cr *Cursor) Fetchs(result interface{}, n int) dbflex.ICursor {
	if cr.Error() != nil {
		cr.SetError(fmt.Errorf("unable to fetch data. %s", cr.Error()))
		return cr
	}

	v := reflect.ValueOf(result)
	if v.Kind() != reflect.Ptr {
		return cr.SetError(fmt.Errorf("result should be a pointer of slice"))
	}
	v = v.Elem()
	if v.Kind() != reflect.Slice {
		return cr.SetError(fmt.Errorf("result should be a pointer of slice"))
	}

	sliceType := v.Type()
	elemType := sliceType.Elem()
	elemIsPtr := elemType.Kind() == reflect.Ptr

	destBuffer := reflect.MakeSlice(sliceType, 1000, 1000)
	read := 0
	used := 0
	for cr.cursor.Next(cr.conn.ctx) {
		destItemValue := createPtrFromType(elemType)
		destItem := destItemValue.Interface()
		err := cr.cursor.Decode(destItem)
		if err != nil {
			cr.SetError(fmt.Errorf("unable to decode cursor data. %s", err.Error()))
			return cr
		}

		if elemIsPtr {
			destBuffer.Index(read).Set(reflect.ValueOf(destItem))
		} else {
			destBuffer.Index(read).Set(reflect.ValueOf(destItem).Elem())
		}

		read++
		used++

		if used == 1000 {
			used = 0
			newLen := read + 1000 - 1
			biggerBuffer := reflect.MakeSlice(sliceType, newLen, newLen)
			reflect.Copy(biggerBuffer, destBuffer)
			destBuffer = biggerBuffer
		}

		if n != 0 && read == n {
			break
		}
	}

	if destBuffer.Len() != read {
		lesseBuffer := reflect.MakeSlice(sliceType, read, read)
		reflect.Copy(lesseBuffer, destBuffer)
		v.Set(lesseBuffer)
	} else {
		v.Set(destBuffer)
	}

	return cr
}
