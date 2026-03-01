# data-mongodb

`data-mongodb` 是 `github.com/infrago/data` 的**mongodb 驱动**。

## 包定位

- 类型：驱动
- 作用：把 `data` 模块的统一接口落到 `mongodb` 后端实现

## 快速接入

```go
import (
    _ "github.com/infrago/data"
    _ "github.com/infrago/data-mongodb"
)
```

```toml
[data]
driver = "mongodb"
```

## `setting` 专用配置项

配置位置：`[data].setting`

- `dsn`
- `database`
- `indexes`
- `cache`
- `errorMode`

## 说明

- `setting` 仅对当前驱动生效，不同驱动键名可能不同
- 连接失败时优先核对 `setting` 中 host/port/认证/超时等参数
