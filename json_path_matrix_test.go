package data_mongodb

import (
	"testing"

	. "github.com/infrago/base"
	"github.com/infrago/data"
	"github.com/infrago/infra"
)

func TestMongoJSONPathExprToFilter(t *testing.T) {
	filter, err := exprToFilter(data.CmpExpr{
		Field: "metadata.name",
		Op:    OpEq,
		Value: "alice",
	})
	if err != nil {
		t.Fatalf("exprToFilter failed: %v", err)
	}
	got, ok := filter["metadata.name"]
	if !ok {
		t.Fatalf("missing json path field in filter: %#v", filter)
	}
	if got != "alice" {
		t.Fatalf("unexpected filter value: %v", got)
	}
}

func TestMongoJSONPathAliasNormalize(t *testing.T) {
	got := normalizeMongoPathAliases("user.metadata.name", []string{"user"})
	if got != "metadata.name" {
		t.Fatalf("unexpected normalized path: %s", got)
	}
}

func TestMongoTxReadOnlyRejectsWrites(t *testing.T) {
	base := &mongoBase{
		inst: &data.Instance{Name: "mongo-readonly"},
		conn: &mongodbConnection{},
		mode: "auto-clear",
	}

	res := base.TxReadOnly(func(tx data.DataBase) Res {
		_, err := tx.Sequence("readonly", 1, 1)
		if err == nil {
			return infra.Fail.With("expected readonly sequence to fail")
		}
		tx.ClearError()
		return infra.OK
	})
	if res.Fail() {
		t.Fatalf("readonly tx callback failed: %s", res.Error())
	}
}
