
### 编译命令
```bash
go build -mod=vendor -o io_bench ./cmd/io_uring_test/
```
> **注意**：因 `iouring-go` 库的 `//go:linkname` 在 Go 1.21+ 被封锁，已 vendor 并修补了 utils.go，用纯 Go type-switch 替代。

### 命令行参数

| 参数 | 默认值 | 说明 |
|---|---|---|
| `-mode` | `both` | `sync` \| `iouring` \| `both` |
| `-bs` | `4k` | 块大小：`4k`、`1m`、`128k` 等 |
| `-c` | `8` | 并发 worker 数 |
| `-d` | `30s` | 每阶段测试时长 |
| `-op` | `randread` | `randread` \| `randwrite` \| `read` \| `write` |
| `-qdepth` | `256` | io_uring submission queue 深度 |
| `-file` | `/tmp/io_bench_data` | 测试文件路径 |
| `-filesize` | `512MiB` | 测试文件大小 |
| `-direct` | `false` | 启用 O_DIRECT 绕过 page cache |
| `-noprep` | `false` | 跳过文件预写（文件已存在时使用）|

### 典型用法
```bash
# 对比 4k 随机读，8 并发，每阶段 30s
./io_bench -mode=both -bs=4k -c=8 -d=30s

# 对比 1M 顺序读，16 并发
./io_bench -mode=both -bs=1m -c=16 -d=30s

# 只跑 io_uring，32 并发，队列深度 512
./io_bench -mode=iouring -bs=4k -c=32 -qdepth=512 -d=60s
```

### 每 5 秒打印的统计指标
`IOPS`、`带宽(MiB/s)`、`错误数`、`CPU%`（用户+内核时间）、`HeapAlloc`、`HeapSys`、`Goroutine 数`、`NumGC`、`GC 累计暂停时间`、`NextGC 阈值`

已进行更改。