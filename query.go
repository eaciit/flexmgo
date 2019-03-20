package flexmgo

import (
	"fmt"
	"strings"

	"git.eaciitapp.com/sebar/dbflex"
	df "git.eaciitapp.com/sebar/dbflex"
	"github.com/eaciit/toolkit"
	. "github.com/eaciit/toolkit"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Query struct {
	dbflex.QueryBase
}

func (q *Query) BuildCommand() (interface{}, error) {
	return nil, nil
}

func (q *Query) BuildFilter(f *df.Filter) (interface{}, error) {
	fm := M{}
	if f.Op == df.OpEq {
		fm.Set(f.Field, M{}.Set("$eq", f.Value))
	} else if f.Op == df.OpNe {
		fm.Set(f.Field, M{}.Set("$ne", f.Value))
	} else if f.Op == df.OpContains {
		fs := f.Value.([]string)
		if len(fs) > 1 {
			bfs := []interface{}{}
			for _, ff := range fs {
				pfm := M{}
				pfm.Set(f.Field, M{}.
					Set("$regex", fmt.Sprintf(".*%s.*", ff)).
					Set("$options", "i"))
				bfs = append(bfs, pfm)
			}
			fm.Set("$or", bfs)
		} else {
			fm.Set(f.Field, M{}.
				Set("$regex", fmt.Sprintf(".*%s.*", fs[0])).
				Set("$options", "i"))
		}
	} else if f.Op == df.OpStartWith {
		fm.Set(f.Field, M{}.
			Set("$regex", fmt.Sprintf("^%s.*$", f.Value)).
			Set("$options", "i"))
	} else if f.Op == df.OpEndWith {
		fm.Set(f.Field, M{}.
			Set("$regex", fmt.Sprintf("^.*%s$", f.Value)).
			Set("$options", "i"))
	} else if f.Op == df.OpIn {
		fm.Set(f.Field, M{}.Set("$in", f.Value))
	} else if f.Op == df.OpNin {
		fm.Set(f.Field, M{}.Set("$nin", f.Value))
	} else if f.Op == df.OpGt {
		fm.Set(f.Field, M{}.Set("$gt", f.Value))
	} else if f.Op == df.OpGte {
		fm.Set(f.Field, M{}.Set("$gte", f.Value))
	} else if f.Op == df.OpLt {
		fm.Set(f.Field, M{}.Set("$lt", f.Value))
	} else if f.Op == df.OpLte {
		fm.Set(f.Field, M{}.Set("$lte", f.Value))
	} else if f.Op == df.OpRange {
		bfs := []*df.Filter{}
		bfs = append(bfs, df.Gte(f.Field, f.Value.([]interface{})[0]))
		bfs = append(bfs, df.Lte(f.Field, f.Value.([]interface{})[1]))
		fand := dbflex.And(bfs...)
		return q.BuildFilter(fand)
	} else if f.Op == df.OpOr || f.Op == df.OpAnd {
		bfs := []interface{}{}
		fs := f.Items
		for _, ff := range fs {
			bf, eb := q.BuildFilter(ff)
			if eb == nil {
				bfs = append(bfs, bf)
			}
		}

		fm.Set(string(f.Op), bfs)
	} else if f.Op == df.OpNot {
		bf, eb := q.BuildFilter(f.Items[0])
		if eb == nil {
			field := f.Items[0].Field
			fm.Set(field, M{}.Set("$not", bf.(M).Get(field)))
		}
	} else {
		return nil, fmt.Errorf("Filter Op %s is not defined", f.Op)
	}
	return fm, nil
}

func (q *Query) Cursor(m M) df.ICursor {
	cursor := new(Cursor)
	cursor.SetThis(cursor)
	conn := q.Connection().(*Connection)

	tablename := q.Config(df.ConfigKeyTableName, "").(string)
	coll := conn.db.Collection(tablename)

	parts := q.Config(df.ConfigKeyGroupedQueryItems, df.GroupedQueryItems{}).(df.GroupedQueryItems)
	where := q.Config(df.ConfigKeyWhere, M{}).(M)
	hasWhere := where != nil

	aggrs, hasAggr := parts[df.QueryAggr]
	groupby, hasGroup := parts[df.QueryGroup]
	//commandParts, hasCommand := parts[df.QueryCommand]
	commandParts, hasCommand := parts[df.QueryCommand]

	if hasAggr {
		pipes := []M{}
		items := aggrs[0].Value.([]*df.AggrItem)
		aggrExpression := M{}
		for _, item := range items {
			if item.Op == df.AggrCount {
				aggrExpression.Set(item.Alias, M{}.Set(string(df.AggrSum), 1))
			} else {
				aggrExpression.Set(item.Alias, M{}.Set(string(item.Op), "$"+item.Field))
			}
		}
		if !hasGroup {
			aggrExpression.Set("_id", "")
		} else {
			groups := func() M {
				s := M{}
				for _, v := range groupby {
					gs := v.Value.([]string)
					for _, g := range gs {
						if strings.TrimSpace(g) != "" {
							s.Set(strings.Replace(g, ".", "_", -1), "$"+g)
						}
					}
				}
				return s
			}()
			aggrExpression.Set("_id", groups)
		}

		if hasWhere {
			pipes = append(pipes, M{}.Set("$match", where))
		}
		pipes = append(pipes, M{}.Set("$group", aggrExpression))
		cur, err := coll.Aggregate(conn.ctx, pipes, new(options.AggregateOptions).SetAllowDiskUse(true))
		if err != nil {
			cursor.SetError(err)
		} else {
			cursor.cursor = cur
			cursor.conn = conn
			cursor.countParm = toolkit.M{}.
				Set("count", tablename).
				Set("query", where)
		}
	} else if hasCommand {
		mCmd := commandParts[0].Value.(toolkit.M)
		cmdObj, _ := mCmd["command"]
		switch cmdObj.(type) {
		case toolkit.M:
			cmdParm := cmdObj.(toolkit.M).Get("commandParm")
			toolkit.Logger().Debugf("count command. %s", toolkit.JsonString(cmdParm))
			curCommand, err := conn.db.RunCommandCursor(conn.ctx,
				toolkit.M{}.Set("count", "testrecord"))
			if err != nil {
				toolkit.Logger().Debugf("error on exec count cursor:. %s", err.Error())
				cursor.SetError(err)
			} else {
				cursor.cursor = curCommand
			}
			return cursor

		default:
			cursor.SetError(toolkit.Errorf("invalid command %v", cmdObj))
			return cursor
		}
		/*
			case "gfsfind":
					b, err := gridfs.NewBucket(conn.db)
					qry := q.db.GridFS(tablename).Find(where)
					cursor.mgocursor = qry
					cursor.mgoiter = qry.Iter()
				case "pipe":
					pipe, ok := m["pipe"]
					if !ok {
						cursor.SetError(toolkit.Errorf("invalid command, calling pipe without pipe data"))
						return cursor
					}

					cursor.mgoiter = coll.Pipe(pipe).AllowDiskUse().Iter()
		*/
		cursor.SetError(fmt.Errorf("pipe and command is not yet applied"))
		return cursor
	} else {
		opt := options.Find()
		if items, ok := parts[df.QuerySelect]; ok {
			fields := items[0].Value.([]string)
			if len(fields) > 0 {
				opt.SetProjection(fields)
			}
		}

		if items, ok := parts[df.QueryOrder]; ok {
			sortKeys := items[0].Value.([]string)
			opt.SetSort(sortKeys)
		}

		if items, ok := parts[df.QuerySkip]; ok {
			skip := items[0].Value.(int64)
			opt.SetSkip(skip)
		}

		if items, ok := parts[df.QueryTake]; ok {
			take := items[0].Value.(int64)
			opt.SetLimit(take)
		}

		var (
			qry *mongo.Cursor
			err error
		)

		qry, err = coll.Find(conn.ctx, where, opt)
		toolkit.Logger().Debugf("querying data. where:%v error:%v", where, err)

		if err != nil {
			cursor.SetError(err)
			return cursor
		}

		cursor.cursor = qry
		cursor.countParm = toolkit.M{}.Set("count", tablename).Set("query", where)
		cursor.conn = conn
	}
	return cursor
}

func (q *Query) Execute(m M) (interface{}, error) {
	tablename := q.Config(df.ConfigKeyTableName, "").(string)
	conn := q.Connection().(*Connection)
	coll := conn.db.Collection(tablename)
	data := m.Get("data")

	parts := q.Config(df.ConfigKeyGroupedQueryItems, df.GroupedQueryItems{}).(df.GroupedQueryItems)
	where := q.Config(df.ConfigKeyWhere, M{}).(M)
	hasWhere := where != nil

	ct := q.Config(df.ConfigKeyCommandType, "N/A")
	switch ct {
	case df.QueryInsert:
		return coll.InsertOne(conn.ctx, data)

	case df.QueryUpdate:
		var err error
		if hasWhere {
			//singleupdate := m.Get("singleupdate", true).(bool)
			singleupdate := false
			if !singleupdate {
				//-- get the field for update
				updateqi, _ := parts[df.QueryUpdate]
				updatevals := updateqi[0].Value.([]string)

				var dataM toolkit.M
				dataM, err = ToM(data)
				dataS := M{}
				if err != nil {
					return nil, err
				}

				if len(updatevals) > 0 {
					for k, v := range dataM {
						for _, u := range updatevals {
							if strings.ToLower(k) == strings.ToLower(u) {
								dataS[k] = v
							}
						}
					}
				} else {
					for k, v := range dataM {
						dataS[k] = v
					}
				}
				//updatedData := toolkit.M{}.Set("$set", dataS)

				_, err = coll.UpdateMany(conn.ctx, where, dataS,
					new(options.UpdateOptions).SetUpsert(true))
			} else {
				_, err = coll.UpdateOne(conn.ctx, where, data,
					new(options.UpdateOptions).SetUpsert(true))
			}
			return nil, err
		} else {
			return nil, toolkit.Errorf("update need to have where clause")
		}

	case df.QueryDelete:
		if hasWhere {
			_, err := coll.DeleteMany(conn.ctx, where)
			return nil, err
		} else {
			return nil, toolkit.Errorf("delete need to have where clause")
		}

	case df.QuerySave:
		whereSave := M{}
		datam, err := toolkit.ToM(data)
		toolkit.Logger().Debugf("M: %s", toolkit.JsonString(data))
		if err != nil {
			return nil, toolkit.Errorf("unable to deserialize data: %s", err.Error())
		}
		if datam.Has("_id") {
			whereSave = M{}.Set("_id", datam.Get("_id"))
		} else {
			return nil, toolkit.Error("_id field is required")
		}

		_, err = coll.UpdateMany(conn.ctx, whereSave,
			toolkit.M{}.Set("$set", datam),
			new(options.UpdateOptions).SetUpsert(true))
		return nil, err
	/*
		case df.QueryCommand:
			commands, ok := parts[df.QueryCommand]
			if !ok {
				return nil, toolkit.Errorf("No command")
			}

			mCommand := commands[0].Value.(toolkit.M)
			cmd, _ := mCommand["command"]

			switch cmd.(type) {
			case string:
				commandTxt := cmd.(string)
				if commandTxt == "" {
					return nil, toolkit.Errorf("No command")
				}

				switch strings.ToLower(commandTxt) {
				case "gfswrite":
					var reader *bufio.Reader
					gfsId, hasId := m["id"]
					gfsMetadata, hasMetadata := m["metadata"]
					gfsFileName := m.GetString("name")
					reader = m.Get("source", nil).(*bufio.Reader)
					if reader == nil {
						return nil, toolkit.Errorf("invalid reader")
					}
					gfsBuffSize := m.Get("size", 1024).(int)

					buff := make([]byte, gfsBuffSize)

					var gfs *mgo.GridFile
					var err error

					//-- check if file exist
					if hasId {
						gfs, err = q.db.GridFS(tablename).OpenId(gfsId)
					} else {
						gfs, err = q.db.GridFS(tablename).Open(gfsFileName)
					}

					//-- if yes remove
					if err == nil {
						gfs.Close()
						if hasId {
							q.db.GridFS(tablename).RemoveId(gfsId)
						} else {
							q.db.GridFS(tablename).Remove(gfsFileName)
						}
					}

					//-- create new one
					gfs, err = q.db.GridFS(tablename).Create(gfsFileName)
					if err != nil {
						return nil, toolkit.Errorf("unable to create GFS %s - %s. %s", tablename, gfsFileName, err.Error())
					}
					defer gfs.Close()

					gfs.SetName(gfsFileName)
					if hasId {
						gfs.SetId(gfsId)
					}
					if hasMetadata {
						gfs.SetMeta(gfsMetadata)
					}
					for {
						nread, err := reader.Read(buff)
						if err != nil && err != io.EOF {
							gfs.Abort()
							return nil, toolkit.Errorf("unable to read file. %s", err.Error())
						}

						if nread == 0 {
							break
						}

						_, err = gfs.Write(buff[:nread])
						if err != nil {
							gfs.Abort()
							return nil, toolkit.Errorf("unable to write to GFS %s - %s. %s", tablename, gfsFileName, err.Error())
						}
					}
					return gfs.Id(), nil

				case "gfsread":
					gfsId, hasId := m["id"]
					gfsFileName := m.GetString("name")
					dest := m.Get("output", &bufio.Writer{}).(*bufio.Writer)
					var gfs *mgo.GridFile
					var err error
					if hasId {
						gfs, err = q.db.GridFS(tablename).OpenId(gfsId)
					} else {
						gfs, err = q.db.GridFS(tablename).Open(gfsFileName)
					}
					if err != nil {
						return nil, toolkit.Errorf("unable to open GFS %s-%s. %s", tablename, gfsFileName, err.Error())
					}
					defer gfs.Close()

					_, err = io.Copy(dest, gfs)
					if err != nil {
						if hasId {
							return nil, toolkit.Errorf("unable to write to output from GFS %s - %s. %s", tablename, gfsId, err.Error())
						} else {
							return nil, toolkit.Errorf("unable to write to output from GFS %s - %s. %s", tablename, gfsFileName, err.Error())
						}
					}

				case "gfsremove", "gfsdelete":
					gfsId, hasId := m["id"]
					gfsFileName := m.GetString("name")

					var err error
					if hasId && gfsId != "" {
						err = q.db.GridFS(tablename).RemoveId(gfsId)
					} else {
						if gfsFileName == "" {
							return nil, toolkit.Errorf("either ID or Name is required for GFS removal")
						}
						err = q.db.GridFS(tablename).Remove(gfsFileName)
					}
					return nil, err

				case "gfstruncate":
					qry := q.db.GridFS(tablename).Find(nil)
					iter := qry.Iter()
					defer iter.Close()
					for {
						m := toolkit.M{}
						if !iter.Next(&m) {
							break
						}

						id := m.Get("_id")
						q.db.GridFS(tablename).RemoveId(id)
					}

				default:
					return nil, toolkit.Errorf("Invalid command: %v", commandTxt)
				}

			case toolkit.M:
				cmdM := cmd.(toolkit.M)
				out := toolkit.M{}
				err := q.db.Run(cmdM, &out)
				return out, err
			default:
				return nil, toolkit.Errorf("Unknown command %v", cmd)
			}

	*/

	default:
		return nil, toolkit.Errorf("Unknown command %v", ct)
	}

	return nil, nil
}

/*
func (q *Query) SetThis(q dbflex.IQuery) {
	panic("not implemented")
}

func (q *Query) This() dbflex.IQuery {
	panic("not implemented")
}

func (q *Query) BuildFilter(*dbflex.Filter) (interface{}, error) {
	panic("not implemented")
}

func (q *Query) BuildCommand() (interface{}, error) {
	panic("not implemented")
}

func (q *Query) Cursor(toolkit.M) dbflex.ICursor {
	panic("not implemented")
}

func (q *Query) Execute(toolkit.M) (interface{}, error) {
	panic("not implemented")
}

func (q *Query) SetConfig(string, interface{}) {
	panic("not implemented")
}

func (q *Query) SetConfigM(toolkit.M) {
	panic("not implemented")
}

func (q *Query) Config(string, interface{}) interface{} {
	panic("not implemented")
}

func (q *Query) ConfigRef(string, interface{}, interface{}) {
	panic("not implemented")
}

func (q *Query) DeleteConfig(...string) {
	panic("not implemented")
}

func (q *Query) Connection() dbflex.IConnection {
	panic("not implemented")
}

func (q *Query) SetConnection(dbflex.IConnection) {
	panic("not implemented")
}
*/
