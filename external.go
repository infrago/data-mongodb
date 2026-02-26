package data_mongodb

import (
	"fmt"

	. "github.com/bamgoo/base"
	"github.com/bamgoo/data"
)

type RawExecutor interface {
	Command(Any) Map
	FindRaw(string, Any, ...Map) []Map
	AggregateRaw(string, Any) []Map
}

func AsRawExecutor(db data.DataBase) (RawExecutor, bool) {
	re, ok := db.(RawExecutor)
	return re, ok
}

func Command(db data.DataBase, cmd Any) Map {
	re, ok := AsRawExecutor(db)
	if !ok {
		return nil
	}
	return re.Command(cmd)
}

func FindRaw(db data.DataBase, collection string, filter Any, opts ...Map) []Map {
	re, ok := AsRawExecutor(db)
	if !ok {
		return nil
	}
	return re.FindRaw(collection, filter, opts...)
}

func AggregateRaw(db data.DataBase, collection string, pipeline Any) []Map {
	re, ok := AsRawExecutor(db)
	if !ok {
		return nil
	}
	return re.AggregateRaw(collection, pipeline)
}

func EnsureMongoDriver(db data.DataBase) error {
	if _, ok := AsRawExecutor(db); !ok {
		return fmt.Errorf("data db is not mongodb driver")
	}
	return nil
}
