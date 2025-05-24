package data_mongodb

import (
	"context"
	"errors"

	. "github.com/infrago/base"
	"github.com/infrago/infra"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

type (
	MongodbView struct {
		base   *MongodbBase
		name   string //模型名称
		schema string //schema，库名
		view   string //视图名
		key    string //主键
		fields Vars   //字段定义
	}
)

// 统计数量
// 添加函数支持
// 函数(字段）
func (view *MongodbView) Count(args ...Any) float64 {
	view.base.lastError = nil

	db := view.base.connect.client.Database(view.base.schema)

	querys := []Map{}
	for _, arg := range args {
		if vvs, ok := arg.(Map); ok {
			query := Map{}
			for k, v := range vvs {
				if v == ASC {
				} else if v == DESC {
				} else {
					//默认
					query[k] = v
				}
			}
			if len(query) > 0 {
				querys = append(querys, query)
			}
		}
	}
	query := Map{}
	if len(querys) > 0 {
		query["$or"] = querys
	}

	ctx := context.Background()
	count, err := db.Collection(view.view).CountDocuments(ctx, query)
	if err != nil {
		view.base.errorHandler("data.count.query", err, view.name, err)
		return 0
	}

	return float64(count)
}

// 查询单条
// 171015改成*版
func (view *MongodbView) First(args ...Any) Map {
	view.base.lastError = nil

	db := view.base.connect.client.Database(view.base.schema)

	querys := []Map{}
	sorts := bson.D{}
	for _, arg := range args {
		if vvs, ok := arg.(Map); ok {
			query := Map{}
			for k, v := range vvs {
				if v == ASC {
					sorts = append(sorts, bson.E{k, 1})
				} else if v == DESC {
					sorts = append(sorts, bson.E{k, -1})
				} else {
					//默认
					query[k] = v
				}
			}
			if len(query) > 0 {
				querys = append(querys, query)
			}
		}
	}
	query := Map{}
	if len(querys) > 0 {
		query["$or"] = querys
	}

	opts := options.FindOne()
	opts.SetSort(sorts) // 设置排序

	var res bson.M

	ctx := context.Background()
	err := db.Collection(view.view).FindOne(ctx, query, opts).Decode(&res)
	if err != nil {
		if !errors.Is(err, mongo.ErrNoDocuments) {
			view.base.errorHandler("data.first.find", err, view.name)
		}
		return nil
	}

	//返回值需要处理特殊类型
	result, err := transform(res)
	if err != nil {
		view.base.errorHandler("data.first.transform", err, view.name)
		return nil
	}

	//如果字段，直接返回不包装
	if view.fields == nil || len(view.fields) == 0 {
		return result
	}

	item := Map{}
	//直接使用err=会有问题，总是不会nil，就解析问题
	errm := infra.Mapping(view.fields, result, item, false, true)
	if errm.Fail() {
		view.base.errorHandler("data.first.mapping", errm, view.name)
		return nil
	}

	return item
}

// 查询列表
// 171015改成*版
func (view *MongodbView) Query(args ...Any) []Map {
	view.base.lastError = nil

	db := view.base.connect.client.Database(view.base.schema)

	querys := []Map{}
	sorts := bson.D{}
	for _, arg := range args {
		if vvs, ok := arg.(Map); ok {
			query := Map{}
			for k, v := range vvs {
				if v == ASC {
					sorts = append(sorts, bson.E{k, 1})
				} else if v == DESC {
					sorts = append(sorts, bson.E{k, -1})
				} else {
					//默认
					query[k] = v
				}
			}
			if len(query) > 0 {
				querys = append(querys, query)
			}
		}
	}
	query := Map{}
	if len(querys) > 0 {
		query["$or"] = querys
	}

	opts := options.Find()
	opts.SetSort(sorts) // 设置排序

	ctx := context.Background()
	cursor, err := db.Collection(view.view).Find(ctx, query, opts)
	if err != nil {
		view.base.errorHandler("data.query", err, view.name)
		return []Map{}
	}
	defer cursor.Close(ctx)

	items := []Map{}

	for cursor.Next(ctx) {

		var res bson.M
		if err := cursor.Decode(&res); err != nil {
			view.base.errorHandler("data.query.decode", err, view.name)
			return []Map{}
		}

		//返回值需要处理特殊类型
		result, err := transform(res)
		if err != nil {
			view.base.errorHandler("data.first.transform", err, view.name)
			return []Map{}
		}

		if view.fields == nil || len(view.fields) == 0 {
			items = append(items, result)
		} else {
			item := Map{}
			//直接使用err=会有问题，总是不会nil，就解析问题
			errm := infra.Mapping(view.fields, result, item, false, true)
			if errm.Fail() {
				view.base.errorHandler("data.query.mapping", errm, view.name)
				return nil
			}
			items = append(items, item)
		}
	}

	return items
}

// 分页查询
// 171015更新为字段*版
func (view *MongodbView) Limit(offset, limit Any, args ...Any) (int64, []Map) {
	view.base.lastError = nil

	db := view.base.connect.client.Database(view.base.schema)

	querys := []Map{}
	sorts := bson.D{}

	for _, arg := range args {
		if vvs, ok := arg.(Map); ok {
			query := Map{}
			for k, v := range vvs {
				if v == ASC {
					sorts = append(sorts, bson.E{k, 1})
				} else if v == DESC {
					sorts = append(sorts, bson.E{k, -1})
				} else {
					//默认
					query[k] = v
				}
			}
			if len(query) > 0 {
				querys = append(querys, query)
			}
		}
	}
	query := Map{}
	if len(querys) > 0 {
		query["$or"] = querys
	}

	ctx := context.Background()
	count, err := db.Collection(view.view).CountDocuments(ctx, query)
	if err != nil {
		view.base.errorHandler("data.limit.count", err, view.name, err)
		return 0, []Map{}
	}

	offsetInt64 := int64(0)
	limitInt64 := int64(10)
	if vv, ok := offset.(int64); ok {
		offsetInt64 = vv
	} else if vv, ok := offset.(int); ok {
		offsetInt64 = int64(vv)
	}
	if vv, ok := limit.(int64); ok {
		limitInt64 = vv
	} else if vv, ok := limit.(int); ok {
		limitInt64 = int64(vv)
	}

	opts := options.Find()
	opts.SetSort(sorts)       // 设置排序
	opts.SetSkip(offsetInt64) // 设置跳过的文档数量
	opts.SetLimit(limitInt64) // 设置返回的文档数量

	cursor, err := db.Collection(view.view).Find(ctx, query, opts)
	if err != nil {
		view.base.errorHandler("data.limit", err, view.name)
		return 0, []Map{}
	}
	defer cursor.Close(ctx)

	items := []Map{}

	for cursor.Next(ctx) {

		var res bson.M
		if err := cursor.Decode(&res); err != nil {
			view.base.errorHandler("data.limit.decode", err, view.name)
			return 0, []Map{}
		}

		//返回值需要处理特殊类型
		result, err := transform(res)
		if err != nil {
			view.base.errorHandler("data.first.transform", err, view.name)
			return 0, []Map{}
		}

		if view.fields == nil || len(view.fields) == 0 {
			items = append(items, result)
		} else {

			item := Map{}
			//直接使用err=会有问题，总是不会nil，就解析问题
			errm := infra.Mapping(view.fields, result, item, false, true)
			if errm.Fail() {
				view.base.errorHandler("data.limit.mapping", errm, view.name)
				return 0, []Map{}
			}
			items = append(items, item)
		}
	}

	return count, items
}

// 查询分组
func (view *MongodbView) Group(field string, args ...Any) []Map {
	view.base.lastError = nil

	//暂时不支持

	return []Map{}
}

// 查询唯一对象
// 换成字段*版
func (view *MongodbView) Entity(id Any) Map {
	view.base.lastError = nil

	db := view.base.connect.client.Database(view.base.schema)

	query := Map{}

	if vv, ok := id.(bson.ObjectID); ok {
		query[view.key] = vv
	} else if vv, ok := id.(string); ok {
		oid, err := bson.ObjectIDFromHex(vv)
		if err == nil {
			query[view.key] = oid
		} else {
			query[view.key] = vv
		}
	} else {
		query[view.key] = id
	}

	var res bson.M

	ctx := context.Background()
	err := db.Collection(view.view).FindOne(ctx, query).Decode(&res)
	if err != nil {
		if !errors.Is(err, mongo.ErrNoDocuments) {
			view.base.errorHandler("data.first.find", err, view.name)
		}
		return nil
	}

	//返回值需要处理特殊类型
	result, err := transform(res)
	if err != nil {
		view.base.errorHandler("data.first.transform", err, view.name)
		return nil
	}

	//如果字段，直接返回不包装
	if view.fields == nil || len(view.fields) == 0 {
		return result
	}

	item := Map{}
	//直接使用err=会有问题，总是不会nil，就解析问题
	errm := infra.Mapping(view.fields, result, item, false, true)
	if errm.Fail() {
		view.base.errorHandler("data.entity.mapping", errm, view.name)
		return nil
	}

	return item
}
