package data_mongodb

import (
	"database/sql"
	"errors"
	"fmt"

	. "github.com/infrago/base"
	"github.com/infrago/data"
	"github.com/infrago/infra"
	"github.com/infrago/log"

	"strings"
)

type (
	mongodbTrigger struct {
		Name  string
		Value Map
	}
	MongodbBase struct {
		connect *MongodbConnect

		name   string
		schema string //schema，库名

		//是否手动提交事务，默认false为自动
		//当调用begin时， 自动变成手动提交事务
		//triggers保存待提交的触发器，手动下有效
		manual   bool
		triggers []mongodbTrigger

		lastError error
	}
)

// 记录触发器
func (base *MongodbBase) trigger(name string, values ...Map) {
	if base.manual {
		//手动时保存触发器
		var value Map
		if len(values) > 0 {
			value = values[0]
		}
		base.triggers = append(base.triggers, mongodbTrigger{Name: name, Value: value})
	} else {
		//自动时，直接触发
		data.Trigger(name, values...)
	}

}

// 查询表，支持多个KEY遍历
func (base *MongodbBase) tableConfig(name string) *data.Table {
	keys := []string{
		fmt.Sprintf("%s.%s", base.name, name),
		fmt.Sprintf("*.%s", name),
		name,
	}

	for _, key := range keys {
		if cfg := data.GetTable(key); cfg != nil {
			return cfg
		}
	}

	return nil
}
func (base *MongodbBase) viewConfig(name string) *data.View {
	keys := []string{
		fmt.Sprintf("%s.%s", base.name, name),
		fmt.Sprintf("*.%s", name),
		name,
	}

	for _, key := range keys {
		if cfg := data.GetView(key); cfg != nil {
			return cfg
		}
	}

	return nil
}
func (base *MongodbBase) modelConfig(name string) *data.Model {
	keys := []string{
		fmt.Sprintf("%s.%s", base.name, name),
		fmt.Sprintf("*.%s", name),
		name,
	}

	for _, key := range keys {
		if cfg := data.GetModel(key); cfg != nil {
			return cfg
		}
	}

	return nil
}

func (base *MongodbBase) errorHandler(key string, err error, args ...Any) {
	if err != nil {
		//出错自动取消事务
		base.Cancel()

		errors := []Any{key, err}
		errors = append(errors, args...)

		base.lastError = err
		log.Warning(errors...)
	}
}

// 关闭数据库
func (base *MongodbBase) Close() error {
	base.connect.mutex.Lock()
	base.connect.actives--
	base.connect.mutex.Unlock()

	//好像不太需要做什么，

	return nil
}
func (base *MongodbBase) Erred() error {
	err := base.lastError
	base.lastError = nil
	return err
}

// ID生成器 序列版
func (base *MongodbBase) Serial(key string, start, step int64) int64 {
	return -1 //暂不支持
}

// 删除ID生成器
func (base *MongodbBase) Break(key string) {
	//暂不支持
}

// 获取表对象
func (base *MongodbBase) Table(name string) data.DataTable {
	if config := base.tableConfig(name); config != nil {
		//模式，表名
		schema, table, key := base.schema, name, "_id"

		if config.Schema != "" {
			schema = config.Schema
		}
		if config.Table != "" {
			table = config.Table
		}
		if config.Key != "" {
			key = config.Key
		}

		fields := Vars{
			// "$count": Var{Type: "int", Nullable: true, Name: "统计"},
		}
		for k, v := range config.Fields {
			fields[k] = v
		}

		table = strings.Replace(table, ".", "_", -1)
		return &MongodbTable{
			MongodbView{base, name, schema, table, key, fields},
		}
	} else {
		panic("[数据]表不存在")
	}
}

// 获取模型对象
func (base *MongodbBase) View(name string) data.DataView {
	if config := base.viewConfig(name); config != nil {

		//模式，表名
		schema, view, key := base.schema, name, "_id"
		if config.Schema != "" {
			schema = config.Schema
		}
		if config.View != "" {
			view = config.View
		}
		if config.Key != "" {
			key = config.Key
		}

		fields := Vars{
			// "$count": Var{Type: "int", Nullable: true, Name: "统计"},
		}
		for k, v := range config.Fields {
			fields[k] = v
		}

		view = strings.Replace(view, ".", "_", -1)
		return &MongodbView{
			base, name, schema, view, key, fields,
		}
	} else {
		panic("[数据]视图不存在")
	}
}

// 获取模型对象
func (base *MongodbBase) Model(name string) data.DataModel {
	if config := base.modelConfig(name); config != nil {

		//模式，表名
		schema, model, key := base.schema, name, "_id"
		if config.Schema != "" {
			schema = config.Schema
		}
		if config.Model != "" {
			model = config.Model
		}
		if config.Key != "" {
			key = config.Key
		}

		fields := Vars{
			// "$count": Var{Type: "int", Nullable: true, Name: "统计"},
		}
		for k, v := range config.Fields {
			fields[k] = v
		}

		model = strings.Replace(model, ".", "_", -1)
		return &MongodbModel{
			base, name, schema, model, key, fields,
		}
	} else {
		panic("[数据]模型不存在")
	}
}

//是否开启缓存
// func (base *MongodbBase) Cache(use bool) (DataBase) {
// 	base.caching = use
// 	return base
// }

// 开启手动模式
func (base *MongodbBase) Begin() (*sql.Tx, error) {
	base.lastError = nil
	base.manual = true
	return base.beginTx()
}

// 注意，此方法为实际开始事务
func (base *MongodbBase) beginTx() (*sql.Tx, error) {
	return nil, errors.New("暂不支持事务")
}

// 此为取消事务
func (base *MongodbBase) endTx() error {
	return errors.New("暂不支持事务")
}

// func (base *MongodbBase) beginExec() (MongodbExecutor, error) {
// 	if base.manual {
// 		_, err := base.beginTx()
// 		return base.exec, err
// 	} else {
// 		return base.connect.db, nil
// 	}
// }

// 提交事务
func (base *MongodbBase) Submit() error {
	return errors.New("暂不支持事务")

	// //提交事务后,要把触发器都发掉
	// for _, trigger := range base.triggers {
	// 	data.Trigger(trigger.Name, trigger.Value)
	// }

	// return nil
}

// 取消事务
func (base *MongodbBase) Cancel() error {
	return errors.New("暂不支持事务")
}

// 批量操作，包装事务
func (base *MongodbBase) Batch(next data.BatchFunc) Res {
	return infra.Fail
}
