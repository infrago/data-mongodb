# data-mongodb

`data-mongodb` 是 `data` 模块的 `mongodb` 驱动。

## 安装

```bash
go get github.com/infrago/data@latest
go get github.com/infrago/data-mongodb@latest
```

## 接入

```go
import (
    _ "github.com/infrago/data"
    _ "github.com/infrago/data-mongodb"
    "github.com/infrago/infra"
)

func main() {
    infra.Run()
}
```

## 配置示例

```toml
[data]
driver = "mongodb"
```

## 公开 API（摘自源码）

- `func Driver() data.Driver`
- `func (d *mongodbDriver) Connect(inst *data.Instance) (data.Connection, error)`
- `func (c *mongodbConnection) Open() error`
- `func (c *mongodbConnection) Close() error`
- `func (c *mongodbConnection) Health() data.Health`
- `func (c *mongodbConnection) DB() *sql.DB { return nil }`
- `func (c *mongodbConnection) Dialect() data.Dialect { return mongoDialect{} }`
- `func (c *mongodbConnection) Base(inst *data.Instance) data.DataBase`
- `func (mongoDialect) Name() string             { return "mongodb" }`
- `func (mongoDialect) Quote(s string) string    { return s }`
- `func (mongoDialect) Placeholder(_ int) string { return "?" }`
- `func (mongoDialect) SupportsILike() bool      { return true }`
- `func (mongoDialect) SupportsReturning() bool  { return true }`
- `func (b *mongoBase) Capabilities() data.Capabilities`
- `func (b *mongoBase) WithContext(ctx context.Context) data.DataBase`
- `func (b *mongoBase) WithTimeout(timeout time.Duration) data.DataBase`
- `func (b *mongoBase) Begin() error`
- `func (b *mongoBase) Commit() error`
- `func (b *mongoBase) Rollback() error`
- `func (b *mongoBase) Close() error`
- `func (b *mongoBase) Tx(fn data.TxFunc) error`
- `func (b *mongoBase) TxReadOnly(fn data.TxFunc) error`
- `func (b *mongoBase) Error() error`
- `func (b *mongoBase) ClearError() { b.setError(nil) }`
- `func (b *mongoBase) Parse(args ...Any) (string, []Any)`
- `func (b *mongoBase) Raw(query string, args ...Any) []Map`
- `func (b *mongoBase) Exec(query string, args ...Any) int64`
- `func (b *mongoBase) Command(cmd Any) Map`
- `func (b *mongoBase) FindRaw(collection string, filter Any, opts ...Map) []Map`
- `func (b *mongoBase) AggregateRaw(collection string, pipeline Any) []Map`
- `func (b *mongoBase) Migrate(names ...string)`
- `func (b *mongoBase) MigratePlan(names ...string) data.MigrateReport`
- `func (b *mongoBase) MigrateDiff(names ...string) data.MigrateReport`
- `func (b *mongoBase) MigrateUp(versions ...string)`
- `func (b *mongoBase) MigrateDown(steps int)`
- `func (b *mongoBase) MigrateTo(version string)`
- `func (b *mongoBase) MigrateDownTo(version string)`
- `func (b *mongoBase) Table(name string) data.DataTable`
- `func (b *mongoBase) View(name string) data.DataView`
- `func (b *mongoBase) Model(name string) data.DataModel`

## 排错

- driver 未生效：确认模块段 `driver` 值与驱动名一致
- 连接失败：检查 endpoint/host/port/鉴权配置
