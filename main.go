package main

import (
	ecserver "Corgi/internal/ec/server"
	"Corgi/internal/fs"
	mossrsocket "Corgi/internal/network/rsocket"
	"context"
	"log"
	"net/http"
	"net/http/pprof"
	_ "net/http/pprof"
	"os"
	"runtime"
	"time"
)

func main() {
	StartPprof(":6060")
	
	//读取命令行第一个参数
	if len(os.Args) < 2 {
		println("Usage: corgi <mountdir> [addr]")
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
				var avgBatchCommitLatency time.Duration
				if dOps > 0 {
					avgBatchCommitLatency = time.Duration((cur.BatchCommitLatency - prev.BatchCommitLatency) / dOps)
				}
				log.Printf("[aioFlush] iops=%.1f/s bw=%.3f MiB/s avg_latency=%s avg_alloc_latency=%s avg_batch_commit_latency=%s interval_ops=%d interval_bytes=%d",
					iops, bwMiB, avgLatency, avgAllocLatency, avgBatchCommitLatency, dOps, dBytes)
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


func StartPprof(addr string) {
	// 先别开太重，mutex/block 后面需要再开。
	// 如果后面怀疑锁/chan 阻塞，再打开：
	// runtime.SetBlockProfileRate(1)
	// runtime.SetMutexProfileFraction(10)

	_ = runtime.MemProfileRate // 保持默认即可，默认会采样内存分配。

	mux := http.NewServeMux()

	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

	mux.Handle("/debug/pprof/allocs", pprof.Handler("allocs"))
	mux.Handle("/debug/pprof/block", pprof.Handler("block"))
	mux.Handle("/debug/pprof/goroutine", pprof.Handler("goroutine"))
	mux.Handle("/debug/pprof/heap", pprof.Handler("heap"))
	mux.Handle("/debug/pprof/mutex", pprof.Handler("mutex"))
	mux.Handle("/debug/pprof/threadcreate", pprof.Handler("threadcreate"))

	go func() {
		log.Printf("pprof listening on %s", addr)
		if err := http.ListenAndServe(addr, mux); err != nil {
			log.Printf("pprof server stopped: %v", err)
		}
	}()
}
