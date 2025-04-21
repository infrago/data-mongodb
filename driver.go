package data_mongodb

import (
	"net/url"
	"strings"

	"github.com/infrago/data"
	"github.com/infrago/infra"
)

var (
	SCHEMAS = []string{
		"mgdb://",
		"mongo://",
		"mongodb://",
	}
)

type (
	MongodbDriver struct{}
)

// 驱动连接
func (drv *MongodbDriver) Connect(inst *data.Instance) (data.Connect, error) {

	schema := infra.Name()
	setting := MongodbSetting{}

	if inst.Config.Url == "" {
		inst.Config.Url = "mongodb://127.0.0.1:27017"
	}

	//支持自定义的schema，相当于数据库名
	for _, s := range SCHEMAS {
		if strings.HasPrefix(inst.Config.Url, s) {
			inst.Config.Url = strings.Replace(inst.Config.Url, s, "mongodb://", 1)
		}
	}

	if inst.Config.Schema != "" {
		schema = inst.Config.Schema
	}
	if vv, ok := inst.Setting["schema"].(string); ok && vv != "" {
		schema = vv
	}
	if vv, ok := inst.Setting["database"].(string); ok && vv != "" {
		schema = vv
	}
	if vv, ok := inst.Setting["db"].(string); ok && vv != "" {
		schema = vv
	}

	if inst.Config.Url != "" {
		durl, err := url.Parse(inst.Config.Url)
		if err == nil {
			if len(durl.Path) > 1 {
				schema = durl.Path[1:]
			}
		}
	}

	return &MongodbConnect{
		instance: inst, setting: setting, schema: schema,
	}, nil
}
