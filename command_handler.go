package flexmgo

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strings"

	df "git.kanosolution.net/kano/dbflex"
	"github.com/sebarcode/codekit"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo/gridfs"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func (q *Query) handleExecuteCommand(conn *Connection) (interface{}, error) {
	tablename := q.Config(df.ConfigKeyTableName, "").(string)
	coll := conn.db.Collection(tablename)
	parts := q.Config(df.ConfigKeyGroupedQueryItems, df.QueryItems{}).(df.QueryItems)

	commands, ok := parts[df.QueryCommand]
	if !ok {
		return nil, fmt.Errorf("no command")
	}

	where := q.Config(df.ConfigKeyWhere, codekit.M{}).(codekit.M)
	cmdName := commands.Op
	cmdValue := commands.Value

	var (
		bucket      *gridfs.Bucket
		gfsBuffSize int32
		err         error
		cmdParm     codekit.M
		mOK         bool
	)
	if strings.ToLower(cmdName)[:3] == "gfs" {
		gfsBuffSize = int32(cmdParm.Get("size", 1024).(int))
		bucketOpt := new(options.BucketOptions)
		bucketOpt.SetChunkSizeBytes(gfsBuffSize)
		bucketOpt.SetName(tablename)
		bucket, err = gridfs.NewBucket(conn.db, bucketOpt)
		if err != nil {
			return nil, fmt.Errorf("error prepare GridFS bucket. %s", err.Error())
		}

		cmdParm, mOK = cmdValue.(codekit.M)
		if !mOK {
			cmdParm = codekit.M{}
		}
	}

	switch cmdName {
	case "gfswrite":
		var reader io.Reader
		gfsId, hasId := cmdParm["id"]
		gfsMetadata, hasMetadata := cmdParm["metadata"]
		gfsFileName := cmdParm.GetString("name")
		reader, readerOK := cmdParm.Get("source", nil).(io.Reader)
		if !readerOK {
			return nil, fmt.Errorf("invalid reader")
		}

		//-- check if file exist, delete if already exist
		if hasId {
			bucket.Delete(gfsId)
		}

		if !hasMetadata {
			gfsMetadata = codekit.M{}
		}
		uploadOpt := new(options.UploadOptions)
		uploadOpt.SetMetadata(gfsMetadata)
		if gfsFileName == "" && hasId {
			gfsFileName = gfsId.(string)
		}
		if gfsFileName == "" {
			gfsFileName = codekit.RandomString(32)
		}

		var objId primitive.ObjectID
		if hasId {
			err = bucket.UploadFromStreamWithID(gfsId, gfsFileName, reader, uploadOpt)
		} else {
			objId, err = bucket.UploadFromStream(gfsFileName, reader, uploadOpt)
		}
		if err != nil {
			return nil, fmt.Errorf("error upload file to GridFS. %s", err.Error())
		}
		return objId, nil

	case "gfsread":
		gfsId, hasId := cmdParm["id"]
		gfsFileName := cmdParm.GetString("name")
		if gfsFileName == "" && hasId {
			gfsFileName = gfsId.(string)
		}
		dest := cmdParm.Get("output", &bufio.Writer{}).(io.Writer)
		var err error

		var ds *gridfs.DownloadStream
		if hasId {
			ds, err = bucket.OpenDownloadStream(gfsId)
		} else {
			ds, err = bucket.OpenDownloadStreamByName(gfsFileName)
		}
		defer ds.Close()

		if err != nil {
			return nil, fmt.Errorf("unable to open GFS %s-%s. %s", tablename, gfsFileName, err.Error())
		}
		defer ds.Close()

		io.Copy(dest, ds)
		return nil, nil

	case "gfsremove", "gfsdelete":
		gfsId, hasId := cmdParm["id"]
		var err error
		if hasId && gfsId != "" {
			err = bucket.Delete(gfsId)
		}
		return nil, err

	case "gfstruncate":
		err := bucket.Drop()
		return nil, err

	case "distinct":
		fieldName := ""
		switch cmdValue := cmdValue.(type) {
		case string:
			fieldName = cmdValue

		case codekit.M:
			fieldName = cmdValue.GetString("field")
			if fieldName == "" {
				return nil, errors.New("field attribute is mandatory")
			}

		default:
			return nil, errors.New("distinct only accepts string or codekit.M")
		}
		vs, err := coll.Distinct(conn.ctx, fieldName, where)
		if err != nil {
			return nil, err
		}
		return vs, nil

	default:
		return nil, fmt.Errorf("invalid command: %v", cmdName)
	}
}
