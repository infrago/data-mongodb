package data_mongodb

import (
	"github.com/bamgoo/bamgoo"
	"github.com/bamgoo/data"
)

func Driver() data.Driver {
	return &mongodbDriver{}
}

func init() {
	drv := Driver()
	bamgoo.Register("mongodb", drv)
	bamgoo.Register("mongo", drv)
	bamgoo.Register("mgdb", drv)
}
