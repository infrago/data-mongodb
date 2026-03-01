# data-mongodb

`data` 模块的 MongoDB 驱动。

## 安装

```bash
go get github.com/infrago/data-mongodb
```

## 注册

```go
import (
    "github.com/infrago/data"
    _ "github.com/infrago/data-mongodb"
)
```

驱动名：

- `mongodb`
- `mongo`
- `mgdb`

## 配置

```toml
[data]
driver = "mongodb"
url = "mongodb://127.0.0.1:27017"
schema = "app" # MongoDB database 名

[data.setting]
database = "app" # schema 为空时可用这个
errorMode = "auto-clear" # or sticky
```

## 支持能力

- CRUD：`Insert / InsertMany / Upsert / UpsertMany / Change / Remove / Update / Delete`
- 查询：`Query / First / Count / Aggregate / Group / Slice / Scan / ScanN`
- DSL：比较、逻辑、`$contains/$overlap/$elemMatch`、`$after`、`$group/$agg/$having`
- 更新：`$set/$inc/$unset/$push/$pull/$addToSet/$setPath/$unsetPath`
- 事务：`Begin/Commit/Rollback/Tx`（Mongo Session Transaction）
- Join：
  - `localField + foreignField`
  - `on` 表达式（lookup pipeline + $expr）
- `Raw/Exec`：Mongo 命令模式

## 事务示例

```go
db := data.Base()
defer db.Close()

err := db.Tx(func(tx data.DataBase) error {
    _ = tx.Table("wallet").Update(base.Map{
        "$inc": base.Map{"balance": -100},
    }, base.Map{"id": 1})
    if tx.Error() != nil {
        return tx.Error()
    }

    _ = tx.Table("wallet").Update(base.Map{
        "$inc": base.Map{"balance": 100},
    }, base.Map{"id": 2})
    if tx.Error() != nil {
        return tx.Error()
    }

    return nil
})
_ = err
```

## Join 示例

### 1) localField / foreignField

```go
items := db.View("order").Query(base.Map{
    "$join": []base.Map{
        {
            "from": "users",
            "alias": "u",
            "localField": "order.user_id",
            "foreignField": "u.id",
        },
    },
})
_ = items
```

### 2) on 表达式

```go
items := db.View("order").Query(base.Map{
    "$join": []base.Map{
        {
            "from": "users",
            "alias": "u",
            "on": base.Map{
                "order.user_id": base.Map{"$eq": "$field:u.id"},
            },
        },
    },
})
_ = items
```

## Raw 示例

### aggregate

```go
rows := db.Raw("aggregate orders", []base.Map{
    {"$match": base.Map{"status": "paid"}},
    {"$group": base.Map{"_id": "$user_id", "total": base.Map{"$sum": "$amount"}}},
})
if db.Error() != nil {
    // handle
}
_ = rows
```

## 标准化 Raw API（推荐）

```go
rows := data_mongodb.FindRaw(db, "users", base.Map{"status": "active"}, base.Map{
  "sort": base.Map{"id": base.DESC},
  "limit": 20,
})
if db.Error() != nil {
  // handle
}
_ = rows
```

### find

```go
rows := db.Raw("find users",
    base.Map{"status": "active"},
    base.Map{"sort": base.Map{"id": -1}, "limit": 20},
)
_ = rows
```

### command

```go
rows := db.Raw("command", base.Map{"ping": 1})
_ = rows
```

## Exec 示例

```go
_ = db.Exec("createCollection users")
_ = db.Exec("insertMany users", []base.Map{{"id": 1, "name": "a"}, {"id": 2, "name": "b"}})
_ = db.Exec("updateMany users", base.Map{"status": "active"}, base.Map{"$set": base.Map{"vip": true}})
_ = db.Exec("deleteMany users", base.Map{"status": "deleted"})
```

## Parse 示例

```go
where, _ := db.Parse(base.Map{
    "status": "active",
    "age": base.Map{"$gte": 18},
})
// where 是 Mongo filter 的 JSON 字符串
_ = where
```

## 错误处理

驱动遵循 data 的 Error 风格：

```go
items := db.Table("user").Query(base.Map{"status": "active"})
if db.Error() != nil {
    // handle
}
_ = items
```

## 说明

- `schema` 对应 MongoDB 的 database 名。
- `Raw/Exec` 为 Mongo 命令风格，不是 SQL。
- 事务要求 Mongo 部署支持事务（副本集/分片）。
