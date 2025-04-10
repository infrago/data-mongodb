package data_mongodb

import (
	"errors"

	"go.mongodb.org/mongo-driver/mongo"
	"golang.org/x/net/context"

	. "github.com/infrago/base"
	"github.com/infrago/infra"
)

type (
	MongodbModel struct {
		base   *MongodbBase
		name   string //模型名称
		schema string //schema，库名
		model  string //这里可能是表名，视图名，或是集合名（mongodb)
		key    string //主键
		fields Vars   //字段定义
	}
)

// 查询单条
// 应该不需要用MAP，直接写SQL的
func (model *MongodbModel) First(args ...Any) Map {
	model.base.lastError = nil

	db := model.base.connect.client.Database(model.base.schema)

	query := Map{}
	if len(args) > 0 {
		if vv, ok := args[0].(Map); ok {
			query = vv
		}
	}

	var result Map

	ctx := context.Background()
	err := db.Collection(model.model).FindOne(ctx, query).Decode(&result)
	if err != nil && !errors.Is(err, mongo.ErrNoDocuments) {
		model.base.errorHandler("data.entity.find", err, model.name)
		return nil
	}

	//如果字段，直接返回不包装
	if model.fields == nil || len(model.fields) == 0 {
		return result
	}

	item := Map{}
	//直接使用err=会有问题，总是不会nil，就解析问题
	errm := infra.Mapping(model.fields, result, item, false, true)
	if errm.Fail() {
		model.base.errorHandler("data.entity.mapping", errm, model.name)
		return nil
	}

	return item
}

// 查询列表
func (model *MongodbModel) Query(args ...Any) []Map {
	model.base.lastError = nil

	db := model.base.connect.client.Database(model.base.schema)

	query := Map{}
	if len(args) > 0 {
		if vv, ok := args[0].(Map); ok {
			query = vv
		}
	}

	ctx := context.Background()
	cursor, err := db.Collection(model.model).Find(ctx, query)
	if err != nil {
		model.base.errorHandler("data.query", err, model.name)
		return []Map{}
	}
	defer cursor.Close(ctx)

	items := []Map{}

	for cursor.Next(ctx) {

		var result Map
		if err := cursor.Decode(&result); err != nil {
			model.base.errorHandler("data.query.decode", err, model.name)
			return []Map{}
		}

		if model.fields == nil || len(model.fields) == 0 {
			items = append(items, result)
		} else {

			item := Map{}
			//直接使用err=会有问题，总是不会nil，就解析问题
			errm := infra.Mapping(model.fields, result, item, false, true)
			if errm.Fail() {
				model.base.errorHandler("data.query.mapping", errm, model.name)
				return nil
			}
			items = append(items, item)
		}
	}

	return items
}
