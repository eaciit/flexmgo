package flexmgo_test

import (
	"fmt"
	"testing"

	"git.kanosolution.net/kano/dbflex"
	"github.com/ariefdarmawan/flexmgo"
	"github.com/sebarcode/codekit"
)

func TestElemMatch(t *testing.T) {
	queryFilter := dbflex.And(
		dbflex.Eq("SafeCardType", "Safe"),
		dbflex.Or(
			dbflex.ElemMatch("Dimension", dbflex.Eq("Kind", "Company"), dbflex.Eq("Value", "Kano")),
			dbflex.ElemMatch("Dimension", dbflex.Eq("Kind", "Project"), dbflex.Eq("Value", "Petrosea")),
		))

	q := new(flexmgo.Query)
	qfm, _ := q.BuildFilter(queryFilter)
	fmt.Println("filter", codekit.JsonString(qfm))
}
