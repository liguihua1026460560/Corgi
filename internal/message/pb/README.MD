protoc \
  -I . \
  --go_out=. \
  --go_opt=paths=source_relative \
  internal/message/pb/messages.proto

| 参数                                     | 示例                                                  | 作用                                                   | 你是否需要                |
| -------------------------------------- | --------------------------------------------------- | ---------------------------------------------------- | -------------------- |
| `-I` / `--proto_path`                  | `-I .`                                              | 指定 `.proto` 文件搜索路径。你的 proto 如果 import 其他 proto，也从这里找 | 建议加                  |
| `--go_out`                             | `--go_out=.`                                        | 启用 Go 代码生成，输出到指定目录                                   | 必须                   |
| `--go_opt=paths=source_relative`       | `--go_opt=paths=source_relative`                    | 生成的 `.pb.go` 路径跟 `.proto` 相对路径一致                     | 强烈建议                 |
| `--go_opt=Mxxx.proto=import/path`      | `--go_opt=Mcommon.proto=Corgi/internal/.../pb` | 覆盖某个 imported proto 的 Go import path                 | 多 proto import 时可能需要 |
| `--descriptor_set_out`                 | `--descriptor_set_out=moss.desc`                    | 生成 proto 描述文件，不生成 Go 代码，常用于网关、schema registry、动态解析   | 一般不需要                |
| `--include_imports`                    | `--include_imports`                                 | 配合 `--descriptor_set_out`，把 import 的 proto 也打进描述文件   | 一般不需要                |
| `--include_source_info`                | `--include_source_info`                             | 配合 `--descriptor_set_out`，保留注释、源码位置信息                | 一般不需要                |
| `--go-grpc_out`                        | `--go-grpc_out=.`                                   | 生成 gRPC 服务代码 `_grpc.pb.go`                           | 你现在不用                |
| `--go-grpc_opt=paths=source_relative`  | `--go-grpc_opt=paths=source_relative`               | gRPC 生成文件也按 proto 原路径输出                              | 只有用 gRPC 才需要         |
| `--plugin`                             | `--plugin=protoc-gen-go=/path/protoc-gen-go`        | 手动指定插件路径                                             | 正常不需要                |
| `--experimental_allow_proto3_optional` | `--experimental_allow_proto3_optional`              | 老版本 protoc 用于支持 proto3 optional                      | 新版本一般不需要             |
| `--version`                            | `protoc --version`                                  | 查看 protoc 版本                                         | 排查时用                 |
| `--help`                               | `protoc --help`                                     | 查看当前 protoc 支持的全部参数                                  | 排查时用                 |
