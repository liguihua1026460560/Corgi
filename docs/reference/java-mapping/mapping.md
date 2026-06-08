# Java to Go Structure Mapping

以下为参考 Java 实现到 Go 项目结构的路径映射（重点针对附件中的 server、upload、block device 相关代码）。

## 关键映射

- Java: `com.macrosan.ec.server.ErasureServer`
- Go: `internal/com/macrosan/ec/server/`

- Java: `com.macrosan.ec.server.AioUploadServerHandler`
- Go: `internal/com/macrosan/ec/server/`

- Java: `com.macrosan.fs.BlockDevice`
- Go: `internal/com/macrosan/fs/`

- Java: `com.macrosan.database.rocksdb.*`
- Go: 
  - 主实现：`internal/com/macrosan/database/kv/pebble/`
  - 预留替换：`internal/com/macrosan/database/kv/rocksdbstub/`

- Java: `com.macrosan.message.socketmsg` / `jsonmsg`
- Go: 
  - 协议定义：`api/proto/**`
  - 消息封装：`internal/com/macrosan/message/pb/`

- Java: `com.macrosan.rsocket.server`
- Go: `internal/com/macrosan/network/rsocket/`

## 迁移注意点

1. 将 metadata 与 payload 的 json 序列化改为 protobuf 二进制。
2. 将块分配、offset 统计、fileMeta 持久化从 RocksDB 语义迁移到 Pebble。
3. 保留 KV 接口层，避免业务层直接绑定 Pebble 实现，便于后续替换 RocksDB。
4. request-response 与 request-channel 语义在 rsocket 层按 handler 分发。
