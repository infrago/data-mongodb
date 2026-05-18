package data_mongodb

import (
	"errors"
	"reflect"
	"testing"
	"time"

	. "github.com/infrago/base"
	"github.com/infrago/data"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

func TestWatcherKeysOptions(t *testing.T) {
	base := &mongoBase{
		inst: &data.Instance{Config: data.Config{Watcher: Map{
			"keys": Map{
				"enable": true,
				"batch":  64,
				"max":    128,
			},
		}}},
	}
	if !base.watcherKeysEnabled() {
		t.Fatalf("expected watcher keys enabled")
	}
	if got := base.watcherKeysBatchSize(); got != 64 {
		t.Fatalf("expected batch size 64, got %d", got)
	}
	if got := base.watcherKeysMaxKeys(); got != 128 {
		t.Fatalf("expected max keys 128, got %d", got)
	}
}

func TestWatcherKeysBoolCompatibility(t *testing.T) {
	base := &mongoBase{
		inst: &data.Instance{Config: data.Config{Watcher: Map{
			"keys": true,
		}}},
	}
	if !base.watcherKeysEnabled() {
		t.Fatalf("expected legacy bool watcher keys to stay enabled")
	}
	if base.watcherKeysBatchSize() != 0 || base.watcherKeysMaxKeys() != 0 {
		t.Fatalf("legacy bool watcher keys should not imply batch/max")
	}
}

func TestNormalizeMongoWriteValueCommonTypes(t *testing.T) {
	oid := primitive.NewObjectID()
	if got := normalizeMongoWriteValue(Var{Type: "oid"}, oid.Hex()); got != oid {
		t.Fatalf("expected object id, got %T %#v", got, got)
	}

	decimalRaw := normalizeMongoWriteValue(Var{Type: "decimal128"}, "123.45")
	if _, ok := decimalRaw.(primitive.Decimal128); !ok {
		t.Fatalf("expected decimal128, got %T %#v", decimalRaw, decimalRaw)
	}

	timeRaw := normalizeMongoWriteValue(Var{Type: "datetime"}, "2026-05-15T10:20:30Z")
	if _, ok := timeRaw.(time.Time); !ok {
		t.Fatalf("expected time.Time, got %T %#v", timeRaw, timeRaw)
	}

	binaryRaw := normalizeMongoWriteValue(Var{Type: "bytea"}, "hello")
	bin, ok := binaryRaw.(primitive.Binary)
	if !ok || !reflect.DeepEqual(bin.Data, []byte("hello")) {
		t.Fatalf("expected primitive binary, got %T %#v", binaryRaw, binaryRaw)
	}
}

func TestNormalizeBsonValueCommonTypes(t *testing.T) {
	oid := primitive.NewObjectID()
	if got := normalizeBsonValue(oid); got != oid.Hex() {
		t.Fatalf("expected object id hex, got %#v", got)
	}

	dec, _ := primitive.ParseDecimal128("123.45")
	if got := normalizeBsonValue(dec); got != "123.45" {
		t.Fatalf("expected decimal text, got %#v", got)
	}

	bin := primitive.Binary{Data: []byte("hello")}
	if got := normalizeBsonValue(bin); !reflect.DeepEqual(got, []byte("hello")) {
		t.Fatalf("expected binary bytes, got %#v", got)
	}
}

func TestNormalizeMongoWriteMapUsesChildFields(t *testing.T) {
	oid := primitive.NewObjectID()
	got := normalizeMongoWriteMap(Map{
		"profile": Map{"owner": oid.Hex()},
	}, Vars{
		"profile": Var{Type: "json", Children: Vars{
			"owner": Var{Type: "oid"},
		}},
	})
	profile, ok := got["profile"].(Map)
	if !ok {
		t.Fatalf("expected profile map, got %#v", got["profile"])
	}
	if profile["owner"] != oid {
		t.Fatalf("expected nested object id, got %#v", profile["owner"])
	}
}

func TestClassifyMongoError(t *testing.T) {
	dup := mongo.WriteException{WriteErrors: mongo.WriteErrors{{Code: 11000, Message: "duplicate key"}}}
	if got := classifyMongoError(dup); !errors.Is(got, data.ErrDuplicate) {
		t.Fatalf("expected duplicate classification, got %v", got)
	}

	timeout := mongo.CommandError{Code: 50, Message: "exceeded time limit"}
	if got := classifyMongoError(timeout); !errors.Is(got, data.ErrTimeout) {
		t.Fatalf("expected timeout classification, got %v", got)
	}
}
