package flexmgo

import (
	"errors"
	"fmt"
	"strings"

	"git.kanosolution.net/kano/dbflex"
	df "git.kanosolution.net/kano/dbflex"
	"github.com/ariefdarmawan/serde"
	"github.com/sebarcode/codekit"
	. "github.com/sebarcode/codekit"

	"go.mongodb.org/mongo-driver/bson"
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
		fm.Set(f.Field, f.Value)
	} else if f.Op == df.OpNe {
		fm.Set(f.Field, M{}.Set("$ne", f.Value))
	} else if f.Op == "$text" {
		fm.Set("$text", M{}.Set("$search", f.Value))
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
			Set("$regex", fmt.Sprintf("^%s", f.Value)).
			Set("$options", "i"))
	} else if f.Op == df.OpEndWith {
		fm.Set(f.Field, M{}.
			Set("$regex", fmt.Sprintf("%s$", f.Value)).
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
	} else if f.Op == df.OpAll {
		values, ok := f.Value.([]interface{})
		if !ok {
			return nil, fmt.Errorf("fail to translate %s. %s", f.Op, JsonString(f))
		}
		fm.Set(f.Field, M{}.Set("$all", values))
	} else if f.Op == df.OpElemMatch {
		matchM := M{}
		for _, item := range f.Items {
			bf, eb := q.BuildFilter(item)
			if eb != nil {
				return nil, fmt.Errorf("error translate filter %s", JsonString(item))
			}
			bfm := bf.(codekit.M)
			for k, v := range bfm {
				matchM.Set(k, v)
			}
		}
		fm.Set(f.Field, M{}.Set("$elemMatch", matchM))
	} else {
		return nil, fmt.Errorf("filter op %s is not defined", f.Op)
	}
	return fm, nil
}

func (q *Query) Cursor(m M) df.ICursor {
	cursor := new(Cursor)
	cursor.SetThis(cursor)
	conn := q.Connection().(*Connection)

	tablename := q.Config(df.ConfigKeyTableName, "").(string)
	coll := conn.db.Collection(tablename)

	parts := q.Config(df.ConfigKeyGroupedQueryItems, df.QueryItems{}).(df.QueryItems)
	where := q.Config(df.ConfigKeyWhere, M{}).(M)
	hasWhere := where != nil

	aggrs, hasAggr := parts[df.QueryAggr]
	groupby, hasGroup := parts[df.QueryGroup]
	commandParts, hasCommand := parts[df.QueryCommand]

	// run simple aggregation
	if hasAggr {
		pipes := []M{}
		items := aggrs.Value.([]*df.AggrItem)
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
				gs := groupby.Value.([]string)
				for _, g := range gs {
					if strings.TrimSpace(g) != "" {
						s.Set(strings.Replace(g, ".", "_", -1), "$"+g)
					}
				}
				return s
			}()
			aggrExpression.Set("_id", groups)
		}

		if hasWhere {
			//fmt.Println("filters:", codekit.JsonString(where))
			pipes = append(pipes, M{}.Set("$match", where))
		}
		pipes = append(pipes, M{}.Set("$group", aggrExpression))
		var cur *mongo.Cursor
		err := wrapTx(conn, func(ctx mongo.SessionContext) error {
			var err error
			cur, err = coll.Aggregate(ctx, pipes, new(options.AggregateOptions).SetAllowDiskUse(true))
			return err
		})
		if err != nil {
			cursor.SetError(err)
		} else {
			cursor.cursor = cur
			cursor.conn = conn
			if len(where) == 0 {
				cursor.countParm = codekit.M{}.
					Set("count", tablename)
			} else {
				cursor.countParm = codekit.M{}.
					Set("count", tablename).
					Set("query", where)
			}
		}

		// run a specific command
	} else if hasCommand {
		cmdName := commandParts.Op
		cmdValue := commandParts.Value
		switch cmdName {
		case "aggregate", "aggr", "pipe":
			pipes := []codekit.M{}
			if hasWhere && len(where) > 0 {
				pipes = append(pipes, M{}.Set("$match", where))
			}

			pipeM := cmdValue
			var (
				pipeMs []codekit.M
				//ok     bool
				cur *mongo.Cursor
				err error
			)

			serde.Serde(pipeM, &pipeMs)
			if len(pipeMs) > 0 {
				if _, has := pipeMs[0]["$text"]; has {
					pipes = pipeMs
				} else {
					pipes = append(pipes, pipeMs...)
				}
			}

			if cur, err = coll.Aggregate(conn.ctx, pipes, new(options.AggregateOptions).SetAllowDiskUse(true)); err != nil {
				cursor.SetError(err)
				return cursor
			}

			cursor.cursor = cur
			cursor.conn = conn
			cursor.countParm = nil
			return cursor

		case "command":
			var curCommand *mongo.Cursor
			err := wrapTx(conn, func(ctx mongo.SessionContext) error {
				var err error
				curCommand, err = conn.db.RunCommandCursor(ctx, cmdValue)
				return err
			})
			if err != nil {
				cursor.SetError(err)
			} else {
				cursor.cursor = curCommand
				cursor.conn = conn
				cursor.countParm = nil
			}
			return cursor

		default:
			cursor.SetError(fmt.Errorf("invalid command %v", cmdName))
			return cursor
		}

		// basic find
	} else {
		opt := options.Find()
		if items, ok := parts[df.QuerySelect]; ok {
			if fields, ok := items.Value.([]string); ok {
				if len(fields) > 0 {
					projection := codekit.M{}
					for _, field := range fields {
						if strings.Contains(field, ":") {
							alias := strings.Split(field, ":")
							if len(alias) <= 1 {
								cursor.SetError(errors.New("error on translating projection field " + field))
								return cursor
							}
							projection.Set(alias[0], alias[1])
						} else {
							projection.Set(field, 1)
						}
					}
					opt.SetProjection(projection)
				}
			}
		}

		if items, ok := parts[df.QueryOrder]; ok {
			sortKeys := items.Value.([]string)
			sortDoc := bson.D{}
			for _, key := range sortKeys {
				if key[0] == '-' {
					sortDoc = append(sortDoc, bson.E{key[1:], -1})
				} else {
					sortDoc = append(sortDoc, bson.E{key, 1})
				}
			}

			if len(sortDoc) > 0 {
				opt.SetSort(sortDoc)
			}
		}

		if items, ok := parts[df.QuerySkip]; ok {
			skip := int64(items.Value.(int))
			opt.SetSkip(skip)
		}

		if items, ok := parts[df.QueryTake]; ok {
			if take, ok := items.Value.(int); ok {
				opt.SetLimit(int64(take))
			}
		}

		var (
			qry *mongo.Cursor
			err error
		)

		err = wrapTx(conn, func(ctx mongo.SessionContext) error {
			var err error
			//fmt.Println(codekit.JsonString(opt.Sort))
			qry, err = coll.Find(ctx, where, opt)
			return err
		})

		if err != nil {
			cursor.SetError(err)
			return cursor
		}

		cursor.cursor = qry
		if len(where) == 0 {
			cursor.countParm = codekit.M{}.Set("count", tablename)
		} else {
			cursor.countParm = codekit.M{}.Set("count", tablename).Set("query", where)
		}
		cursor.conn = conn
	}
	return cursor
}

func (q *Query) Execute(m M) (interface{}, error) {
	tablename := q.Config(df.ConfigKeyTableName, "").(string)
	conn := q.Connection().(*Connection)
	coll := conn.db.Collection(tablename)
	data := m.Get("data")

	parts := q.Config(df.ConfigKeyGroupedQueryItems, df.QueryItems{}).(df.QueryItems)
	where := q.Config(df.ConfigKeyWhere, M{}).(M)
	hasWhere := where != nil

	ct := q.Config(df.ConfigKeyCommandType, "N/A")
	switch ct {
	case df.QueryInsert:
		var res *mongo.InsertOneResult
		dataM, _ := codekit.ToMTag(data, conn.FieldNameTag())
		err := wrapTx(conn, func(ctx mongo.SessionContext) error {
			var err error
			res, err = coll.InsertOne(ctx, dataM)
			return err
		})
		if err != nil {
			return nil, err
		}
		dataM.Set("_id", res.InsertedID)
		return res.InsertedID, nil

	case df.QueryUpdate:
		var err error
		if hasWhere {
			singleupdate := m.Get("singleupdate", false).(bool)
			//singleupdate := false
			if !singleupdate {
				//-- get the field for update
				updateqi, _ := parts[df.QueryUpdate]
				updatevals := updateqi.Value.([]string)

				var dataM codekit.M
				dataM, err = codekit.ToMTag(data, conn.FieldNameTag())
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
				//updatedData := codekit.M{}.Set("$set", dataS)

				err = wrapTx(conn, func(ctx mongo.SessionContext) error {
					_, err := coll.UpdateMany(ctx, where,
						codekit.M{}.Set("$set", dataS),
						new(options.UpdateOptions).SetUpsert(false))
					return err
				})
			} else {
				err = wrapTx(conn, func(ctx mongo.SessionContext) error {
					_, err := coll.UpdateOne(ctx, where,
						codekit.M{}.Set("$set", data),
						new(options.UpdateOptions).SetUpsert(false))
					return err
				})
			}
			return nil, err
		} else {
			return nil, fmt.Errorf("update need to have where clause")
		}

	case df.QueryDelete:
		if hasWhere {
			err := wrapTx(conn, func(ctx mongo.SessionContext) error {
				_, err := coll.DeleteMany(ctx, where)
				return err
			})
			return nil, err
		} else {
			return nil, fmt.Errorf("delete need to have where clause. To delete all data in a collection, please use DropTable instead of Delete")
		}

	case df.QuerySave:
		whereSave := M{}
		datam, err := codekit.ToMTag(data, conn.FieldNameTag())
		if err != nil {
			return nil, fmt.Errorf("unable to deserialize data: %s", err.Error())
		}
		if datam.Has("_id") {
			whereSave = M{}.Set("_id", datam.Get("_id"))
		} else {
			return nil, errors.New("_id field is required")
		}

		err = wrapTx(conn, func(ctx mongo.SessionContext) error {
			_, err := coll.ReplaceOne(ctx, whereSave, datam,
				new(options.ReplaceOptions).SetUpsert(true))
			return err
		})
		return nil, err

	case df.QueryCommand:
		return q.handleExecuteCommand(conn)
	}
	return nil, nil
}

func wrapTx(conn *Connection, fn func(ctx mongo.SessionContext) error) error {
	var err error
	if conn.sess != nil {
		err = mongo.WithSession(conn.ctx, conn.sess, func(sc mongo.SessionContext) error {
			return fn(sc)
		})
	} else {
		err = fn(nil)
	}
	return err
}
