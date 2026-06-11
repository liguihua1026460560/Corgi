package main

import (
	"context"
	"log"
	ecserver "mossserver/internal/com/macrosan/ec/server"
	"mossserver/internal/com/macrosan/fs"
	mossrsocket "mossserver/internal/com/macrosan/network/rsocket"
	"net/http"
	_ "net/http/pprof"
	"os"
	"time"
)

func main() {
	go func() {
		_ = http.ListenAndServe("127.0.0.1:6060", nil)
	}()
	//读取命令行第一个参数
	if len(os.Args) < 2 {
		println("Usage: mossserver <mountdir> [addr]")
		return
	}
	mountDir := os.Args[1]
	addr := ":11115"
	if len(os.Args) > 2 {
		addr = os.Args[2]
	}

	ctx := context.Background()
	if err := fs.Init(ctx, mountDir); err != nil {
		log.Fatal(err)
	}

	var device *fs.BlockDevice
	for _, d := range fs.Devices() {
		device = d
		break
	}
	if device == nil {
		log.Fatalf("no block device found under mount dir %q", mountDir)
	}

	// 每隔 5s 打印一次 AIO flush 的 IOPS、带宽、平均时延。
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		prev := ecserver.SnapshotWriteStats()
		prevAt := time.Now()
		for {
			select {
			case <-ctx.Done():
				return
			case t := <-ticker.C:
				cur := ecserver.SnapshotWriteStats()
				secs := t.Sub(prevAt).Seconds()
				dOps := cur.Ops - prev.Ops
				dBytes := cur.Bytes - prev.Bytes
				dNanos := cur.Nanos - prev.Nanos
				dAllocLatency := cur.AllocLatency - prev.AllocLatency
				iops := float64(dOps) / secs
				bwMiB := float64(dBytes) / secs / (1024 * 1024)
				var avgLatency time.Duration
				if dOps > 0 {
					avgLatency = time.Duration(dNanos / dOps)
				}
				var avgAllocLatency time.Duration
				if dOps > 0 {
					avgAllocLatency = time.Duration(dAllocLatency / dOps)
				}
				log.Printf("[aioFlush] iops=%.1f/s bw=%.3f MiB/s avg_latency=%s avg_alloc_latency=%s interval_ops=%d interval_bytes=%d",
					iops, bwMiB, avgLatency, avgAllocLatency, dOps, dBytes)
				prev = cur
				prevAt = t
			}
		}
	}()

	server := mossrsocket.NewErasureServer(device)
	if err := mossrsocket.StartRSocketServer(ctx, addr, server); err != nil {
		log.Fatal(err)
	}

}
