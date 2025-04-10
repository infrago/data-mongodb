package data_mongodb

import (
	"errors"
	"fmt"
	"time"

	. "github.com/infrago/base"
	"github.com/infrago/data"
	"github.com/infrago/infra"
	"go.mongodb.org/mongo-driver/v2/bson"
	"golang.org/x/net/context"
)

type (
	MongodbTable struct {
		MongodbView
	}
)

// 创建对象
func (table *MongodbTable) Create(dddd Map) Map {
	table.base.lastError = nil
	db := table.base.connect.client.Database(table.base.schema)

	value := Map{}
	if table.fields != nil && len(table.fields) > 0 {
		//按字段生成值
		errm := infra.Mapping(table.fields, dddd, value, false, false)
		if errm.Fail() {
			table.base.errorHandler("data.create.parse", errm, errm.Args, table.name, value)
			return nil
		}
	} else {
		value = dddd
	}

	ctx := context.Background()
	res, err := db.Collection(table.view).InsertOne(ctx, value)
	if err != nil {
		table.base.errorHandler("data.create.insert", err, table.name, value)
		return nil
	}

	value[table.key] = res.InsertedID

	//触发器
	table.base.trigger(data.CreateTrigger, Map{"base": table.base.name, "table": table.name, "entity": value})

	return value
}

// 修改对象
func (table *MongodbTable) Change(item Map, dddd Map) Map {
	table.base.lastError = nil
	db := table.base.connect.client.Database(table.base.schema)

	if item == nil || item[table.key] == nil {
		table.base.errorHandler("data.change.empty", errors.New("无效数据"), table.name)
		return nil
	}

	value := Map{}
	if table.fields != nil && len(table.fields) > 0 {
		//按字段生成值
		errm := infra.Mapping(table.fields, dddd, value, true, false)
		if errm.Fail() {
			table.base.errorHandler("data.change.parse", errm, table.name)
			return nil
		}
	} else {
		for k, v := range dddd {
			if k != INC {
				value[k] = v
			}

		}
	}

	//记录修改时间
	if _, ok := table.fields["changed"]; ok && dddd["changed"] == nil {
		dddd["changed"] = time.Now()
	}

	querys := Map{
		table.key: item[table.key],
	}
	changes := Map{
		"$set": value,
	}
	if dddd["$inc"] != nil {
		changes["$inc"] = dddd["$inc"]
	}
	if dddd[INC] != nil {
		changes["$inc"] = dddd[INC]
	}

	fmt.Println("changes", changes)

	ctx := context.Background()
	_, err := db.Collection(table.view).UpdateOne(ctx, querys, changes)
	if err != nil {
		table.base.errorHandler("data.change.update", err, table.name, querys, changes)
		return nil
	}

	// 不改item
	// 先复制item
	newItem := Map{}
	for k, v := range item {
		newItem[k] = v
	}
	for k, v := range value {
		newItem[k] = v
	}

	table.base.trigger(data.ChangeTrigger, Map{"base": table.base.name, "table": table.name, "entity": newItem, "before": item, "after": newItem})

	return newItem
}

// 逻辑删除和恢复已经抛弃
// 这两个功能应该是逻辑层干的事，不应和驱动混在一起
// 此为物理删除单条记录，并返回记录，所以要先查询单条
func (table *MongodbTable) Remove(args ...Any) Map {
	table.base.lastError = nil

	if len(args) == 0 {
		table.base.errorHandler("data.remove.empty", errors.New("无效数据"), table.name)
		return nil
	}

	db := table.base.connect.client.Database(table.base.schema)

	var obj Map
	if len(args) > 0 {
		if vv, ok := args[0].(Map); ok {
			obj = vv
		} else if vv, ok := args[0].(string); ok {
			obj = Map{table.key: vv}
		}
	}

	querys := Map{}
	if oid, ok := obj[table.key].(bson.ObjectID); ok {
		querys[table.key] = oid
	} else if vv, ok := obj[table.key].(string); ok {
		idid, erer := bson.ObjectIDFromHex(vv)
		if erer == nil {
			querys[table.key] = idid
		} else {
			querys[table.key] = vv
		}
	}

	//先查询数据
	item := table.Entity(querys[table.key])
	if item == nil {
		table.base.errorHandler("data.remove.empty", errors.New("无效数据"), table.name, querys)
		return nil
	}

	ctx := context.Background()
	_, err := db.Collection(table.view).DeleteOne(ctx, querys)
	if err != nil {
		table.base.errorHandler("data.remove.delete", err, table.name, querys)
		return nil
	}

	//注意这里，如果手动提交事务， 那这里直接返回，是不需要提交的
	table.base.trigger(data.RemoveTrigger, Map{"base": table.base.name, "table": table.name, "entity": item})

	return item
}

// 批量删除，这可是真删
func (table *MongodbTable) Delete(args ...Any) int64 {
	table.base.lastError = nil

	if len(args) == 0 {
		table.base.errorHandler("data.delete.empty", errors.New("无效数据"), table.name)
		return 0
	}

	db := table.base.connect.client.Database(table.base.schema)

	querys := Map{}
	if vv, ok := args[0].(Map); ok {
		querys = vv
	} else if vv, ok := args[0].(string); ok {
		idid, erer := bson.ObjectIDFromHex(vv)
		if erer == nil {
			querys[table.key] = idid
		} else {
			querys[table.key] = vv
		}
	} else if vv, ok := args[0].(bson.ObjectID); ok {
		querys[table.key] = vv
	}

	ctx := context.Background()
	res, err := db.Collection(table.view).DeleteOne(ctx, querys)
	if err != nil {
		table.base.errorHandler("data.remove.delete", err, table.name, querys)
		return 0
	}

	//注意这里，如果手动提交事务， 那这里直接返回，是不需要提交的
	table.base.trigger(data.RemoveTrigger, Map{"base": table.base.name, "table": table.name})

	return res.DeletedCount
}

// 批量更新，直接更了， 没有任何relate相关处理的
func (table *MongodbTable) Update(update Map, args ...Any) int64 {
	table.base.lastError = nil

	//按字段生成值
	value := Map{}
	errm := infra.Mapping(table.fields, update, value, true, false)
	if errm.Fail() {
		table.base.errorHandler("data.delete.mapping", errm, table.name)
		return int64(0)
	}

	db := table.base.connect.client.Database(table.base.schema)

	querys := Map{}
	if vv, ok := args[0].(Map); ok {
		querys = vv
	} else if vv, ok := args[0].(string); ok {
		idid, erer := bson.ObjectIDFromHex(vv)
		if erer == nil {
			querys[table.key] = idid
		} else {
			querys[table.key] = vv
		}
	} else if vv, ok := args[0].(bson.ObjectID); ok {
		querys[table.key] = vv
	}

	changes := Map{
		"$set": value,
	}
	if update["$inc"] != nil {
		changes["$inc"] = update["$inc"]
	}
	if update[INC] != nil {
		changes["$inc"] = update[INC]
	}

	ctx := context.Background()
	res, err := db.Collection(table.view).UpdateMany(ctx, querys, changes)
	if err != nil {
		table.base.errorHandler("data.remove.update", err, table.name, querys)
		return 0
	}

	return res.ModifiedCount
}
