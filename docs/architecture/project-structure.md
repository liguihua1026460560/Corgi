# Project Structure

## 设计原则

1. 目录结构优先模拟 Java 路径，便于对照迁移。
2. 网络、协议、存储引擎、alloc 分层解耦。
3. 通过 `database/kv` 抽象层实现 Pebble 主实现、RocksDB 预留替换。
4. 全量消息定义放入 `api/proto`，业务只依赖生成后的 pb 层。

## 顶层目录

- `cmd/mossserver`：服务启动入口（预留）
- `api/proto`：protobuf 协议定义（common/meta/storage）
- `configs`：配置模板与环境配置（预留）
- `deployments`：部署清单（预留）
- `internal`：核心业务实现（按 Java 风格映射）
- `pkg/protocol`：对外可复用协议适配层（预留）
- `scripts`：构建、生成、检查脚本（预留）
- `test`：集成与基准测试目录（预留）

## internal 关键分层

- `internal/com/macrosan/network/rsocket`：go-rsocket 服务端接入层
- `internal/com/macrosan/message/pb`：pb 消息封装与转换
- `internal/com/macrosan/database/kv/pebble`：Pebble 实现（主）
- `internal/com/macrosan/database/kv/rocksdbstub`：RocksDB 适配预留（替换位）
- `internal/com/macrosan/database/alloc`：磁盘空间分配 alloc 与回收策略
- `internal/com/macrosan/ec/server`：对应 Java `ec/server` 的服务处理层
- `internal/com/macrosan/fs`：块设备与文件系统抽象
- `internal/com/macrosan/storage`：存储池与写入路径管理

## protobuf 拆分建议

- `api/proto/common`：错误码、基础头、通用字段
- `api/proto/meta`：元数据、inode、bucket、quota
- `api/proto/storage`：对象读写、分片、迁移、重建
