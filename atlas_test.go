package flexmgo_test

import (
	"testing"

	"git.kanosolution.net/kano/dbflex"
	"github.com/sebarcode/codekit"
	"github.com/smartystreets/goconvey/convey"
)

func TestAtlas(t *testing.T) {
	convey.Convey("prepare db", t, func() {
		connTxt := "mongodb+srv://readonly:Password.1@cluster0.lobvo.mongodb.net/appdb?retryWrites=true&w=majority"
		conn, err := dbflex.NewConnectionFromURI(connTxt, nil)
		convey.So(err, convey.ShouldBeNil)

		convey.Convey("connect", func() {
			err = conn.Connect()
			convey.So(err, convey.ShouldBeNil)
			defer conn.Close()
			conn.SetFieldNameTag("json")

			convey.Convey("read data", func() {
				dest := codekit.M{}
				err = conn.Cursor(dbflex.From("info").Select().Take(1), nil).Fetch(&dest).Error()
				convey.So(err, convey.ShouldBeNil)
				convey.So(dest.GetInt("Version"), convey.ShouldNotEqual, 0)
				convey.Println()
				convey.Println(codekit.JsonString(dest))
			})
		})
	})
}
