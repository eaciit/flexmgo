package flexmgo

import "git.eaciitapp.com/sebar/dbflex"

func init() {
	dbflex.RegisterDriver("mongodb", func(si *dbflex.ServerInfo) dbflex.IConnection {
		c := new(Connection)
		c.ServerInfo = *si
		c.SetThis(c)
		return c
	})
}
