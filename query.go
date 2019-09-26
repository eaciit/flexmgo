package flexmgo

import (
	"fmt"
	"io"
	"strings"
	"time"

	"git.eaciitapp.com/sebar/dbflex"
	df "git.eaciitapp.com/sebar/dbflex"
	"github.com/eaciit/toolkit"
	. "github.com/eaciit/toolkit"

	"bufio"

	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/gridfs"
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
		fs := f.Value.([]interface{})
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
			//cmdParm := cmdObj.(toolkit.M).Get("commandParm")
			curCommand, err := conn.db.RunCommandCursor(conn.ctx, cmdObj)
			if err != nil {
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
		//toolkit.Logger().Debugf("querying data. where:%v error:%v", where, err)

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
		res, err := coll.InsertOne(conn.ctx, data)
		if err != nil {
			return nil, err
		}
		return res.InsertedID, nil

	case df.QueryUpdate:
		var err error
		if hasWhere {
			singleupdate := m.Get("singleupdate", false).(bool)
			//singleupdate := false
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

				_, err = coll.UpdateMany(conn.ctx, where,
					toolkit.M{}.Set("$set", dataS),
					new(options.UpdateOptions).SetUpsert(false))
			} else {
				_, err = coll.UpdateOne(conn.ctx, where,
					toolkit.M{}.Set("$set", data),
					new(options.UpdateOptions).SetUpsert(false))
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
			return nil, toolkit.Errorf("delete need to have where clause. For delete all data in a collection, please use DropTable instead of Delete")
		}

	case df.QuerySave:
		whereSave := M{}
		datam, err := toolkit.ToM(data)
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

			var (
				bucket      *gridfs.Bucket
				gfsBuffSize int32
				err         error
			)
			if strings.ToLower(commandTxt)[:3] == "gfs" {
				gfsBuffSize = int32(m.Get("size", 1024).(int))
				bucketOpt := new(options.BucketOptions)
				bucketOpt.SetChunkSizeBytes(gfsBuffSize)
				bucketOpt.SetName(tablename)
				bucket, err = gridfs.NewBucket(conn.db, bucketOpt)
				if err != nil {
					return nil, toolkit.Errorf("error prepare GridFS bucket. %s", err.Error())
				}
			}

			switch strings.ToLower(commandTxt) {
			case "gfswrite":
				var reader io.Reader
				gfsId, hasId := m["id"]
				gfsMetadata, hasMetadata := m["metadata"]
				gfsFileName := m.GetString("name")
				reader = m.Get("source", nil).(io.Reader)
				if reader == nil {
					return nil, toolkit.Errorf("invalid reader")
				}

				//-- check if file exist, delete if already exist
				if hasId {
					bucket.Delete(gfsId)
				}

				if !hasMetadata {
					gfsMetadata = toolkit.M{}
				}
				uploadOpt := new(options.UploadOptions)
				uploadOpt.SetMetadata(gfsMetadata)
				if gfsFileName == "" && hasId {
					gfsFileName = gfsId.(string)
				}
				if gfsFileName == "" {
					gfsFileName = toolkit.RandomString(32)
				}

				var objId primitive.ObjectID
				if hasId {
					err = bucket.UploadFromStreamWithID(gfsId, gfsFileName, reader, uploadOpt)
				} else {
					objId, err = bucket.UploadFromStream(gfsFileName, reader, uploadOpt)
				}
				if err != nil {
					return nil, toolkit.Errorf("error upload file to GridFS. %s", err.Error())
				}
				return objId, nil

			case "gfsread":
				gfsId, hasId := m["id"]
				gfsFileName := m.GetString("name")
				if gfsFileName == "" && hasId {
					gfsFileName = gfsId.(string)
				}
				dest := m.Get("output", &bufio.Writer{}).(io.Writer)
				var err error

				var ds *gridfs.DownloadStream
				if hasId {
					ds, err = bucket.OpenDownloadStream(gfsId)
				} else {
					ds, err = bucket.OpenDownloadStreamByName(gfsFileName)
				}
				defer ds.Close()

				if err != nil {
					return nil, toolkit.Errorf("unable to open GFS %s-%s. %s", tablename, gfsFileName, err.Error())
				}
				defer ds.Close()

				io.Copy(dest, ds)
				return nil, nil

			case "gfsremove", "gfsdelete":
				gfsId, hasId := m["id"]

				var err error
				if hasId && gfsId != "" {
					err = bucket.Delete(gfsId)
				}
				return nil, err

			case "gfstruncate":
				err := bucket.Drop()
				return nil, err

			case "watch":
				watchFn := m.Get("fn", nil)
				if watchFn == nil {
					return nil, fmt.Errorf("watch need a func(toolkit.M)")
				}

				opt := options.ChangeStream()
				opt.SetBatchSize(1024)
				opt.SetMaxAwaitTime(24 * time.Hour)

				toolkit.Logger().Debugf("prepare to wacth %s", tablename)
				cs, err := coll.Watch(conn.ctx, []toolkit.M{}, opt)
				if err != nil {
					toolkit.Logger().Debugf("watch %s has error", tablename, err.Error())
					return nil, err
				}
				toolkit.Logger().Debugf("watch %s is currently running", tablename)

				go func() {
					defer cs.Close(conn.ctx)
					for cs.Next(conn.ctx) {
						data := toolkit.M{}
						if err = cs.Decode(&data); err != nil {
							continue
						}

						watchFn.(func(toolkit.M))(data)
					}
				}()
			default:
				return nil, toolkit.Errorf("Invalid command: %v", commandTxt)
			}

		case toolkit.M:
			cmdM := cmd.(toolkit.M)
			sr := conn.db.RunCommand(conn.ctx, cmdM)
			if sr.Err() != nil {
				return nil, toolkit.Errorf("unablet to run command. %s. Command: %s",
					sr.Err().Error(), toolkit.JsonString(cmdM))
			}
			return sr, nil

		default:
			return nil, toolkit.Errorf("Unknown command %v", cmd)
		}

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
