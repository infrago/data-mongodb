package data_mongodb

import (
	"github.com/infrago/infra"
	"github.com/infrago/data"
)

func Driver() data.Driver {
	return &mongodbDriver{}
}

func init() {
	drv := Driver()
	infra.Register("mongodb", drv)
	infra.Register("mongo", drv)
	infra.Register("mgdb", drv)
}
