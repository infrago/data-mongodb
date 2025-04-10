package data_mongodb

import (
	"github.com/infrago/data"
	"github.com/infrago/infra"
	_ "github.com/lib/pq" //此包自动注册名为mongodb的sql驱动
)

var (
	DRIVERS = []string{
		"mongodbql", "mongodb", "pgsql", "pgdb", "pg",
		"cockroachdb", "cockroach", "crdb",
		"timescaledb", "timescale", "tsdb",
	}
)

// 返回驱动
func Driver() data.Driver {
	return &MongodbDriver{}
}

func init() {
	driver := Driver()
	for _, key := range DRIVERS {
		infra.Register(key, driver)
	}
}
