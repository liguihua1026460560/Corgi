package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"math"
	"net"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	gorsocket "github.com/rsocket/rsocket-go"
	rspayload "github.com/rsocket/rsocket-go/payload"
	"github.com/rsocket/rsocket-go/rx/flux"

	mossrsocket "mossserver/internal/com/macrosan/network/rsocket"
)

type config struct {
	host        string
	port        int
	lun         string
	blockSize   int
	concurrency int
	requests    int
	chunks      int
	filePrefix  string
	metaPrefix  string
	timeout     time.Duration
}

type counters struct {
	uploads atomic.Uint64
	bytes   atomic.Uint64
}

func main() {
	cfg, err := parseConfig(os.Args[1:])
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			os.Exit(0)
		}
		log.Fatal(err)
	}

	ctx := context.Background()
	if cfg.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, cfg.timeout)
		defer cancel()
	}

	addr := net.JoinHostPort(cfg.host, strconv.Itoa(cfg.port))
	client, err := gorsocket.Connect().
		ConnectTimeout(cfg.timeout).
		Transport(gorsocket.TCPClient().SetAddr(addr).Build()).
		Start(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	var stats counters
	start := time.Now()
	if err = runUploads(ctx, client, cfg, &stats, addr); err != nil {
		log.Fatal(err)
	}

	elapsed := time.Since(start)
	bytes := stats.bytes.Load()
	log.Printf("uploads=%d bytes=%s elapsed=%s throughput=%s/s",
		stats.uploads.Load(), formatBytes(bytes), elapsed.Round(time.Millisecond),
		formatBytes(uint64(float64(bytes)/math.Max(elapsed.Seconds(), 1e-9))))
}

func parseConfig(args []string) (config, error) {
	cfg := config{
		host:        "127.0.0.1",
		port:        11115,
		concurrency: runtime.NumCPU(),
		requests:    runtime.NumCPU(),
		chunks:      1,
		filePrefix:  "aio-upload-client",
		metaPrefix:  "aio-upload-client",
		timeout:     30 * time.Second,
	}

	blockSize := "4k"
	flags := flag.NewFlagSet("aio_upload_client", flag.ContinueOnError)
	flags.StringVar(&cfg.host, "ip", cfg.host, "server ip")
	flags.IntVar(&cfg.port, "port", cfg.port, "server port")
	flags.StringVar(&cfg.lun, "lun", cfg.lun, "target lun; empty lets server use its default device")
	flags.StringVar(&blockSize, "block-size", blockSize, "bytes per chunk, e.g. 4k, 1m, 64mb")
	flags.IntVar(&cfg.concurrency, "concurrency", cfg.concurrency, "concurrent upload channels")
	flags.IntVar(&cfg.requests, "requests", cfg.requests, "total upload requests")
	flags.IntVar(&cfg.chunks, "chunks", cfg.chunks, "chunks per upload request")
	flags.StringVar(&cfg.filePrefix, "file-prefix", cfg.filePrefix, "uploaded file name prefix")
	flags.StringVar(&cfg.metaPrefix, "meta-prefix", cfg.metaPrefix, "uploaded meta key prefix")
	flags.DurationVar(&cfg.timeout, "timeout", cfg.timeout, "overall timeout")
	if err := flags.Parse(args); err != nil {
		return config{}, err
	}

	size, err := parseSize(blockSize)
	if err != nil {
		return config{}, fmt.Errorf("invalid -block-size: %w", err)
	}
	if size <= 0 || size > int64(math.MaxInt) {
		return config{}, fmt.Errorf("-block-size out of range: %s", blockSize)
	}
	cfg.blockSize = int(size)

	if cfg.port <= 0 || cfg.port > 65535 {
		return config{}, errors.New("-port must be between 1 and 65535")
	}
	if strings.TrimSpace(cfg.host) == "" {
		return config{}, errors.New("-ip is required")
	}
	if cfg.concurrency <= 0 {
		return config{}, errors.New("-concurrency must be > 0")
	}
	if cfg.requests <= 0 {
		return config{}, errors.New("-requests must be > 0")
	}
	if cfg.chunks <= 0 {
		return config{}, errors.New("-chunks must be > 0")
	}
	return cfg, nil
}

func runUploads(ctx context.Context, client gorsocket.Client, cfg config, stats *counters, addr string) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	jobs := make(chan int)
	errCh := make(chan error, 1)
	var wg sync.WaitGroup

	for i := 0; i < cfg.concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			client2, _ := gorsocket.Connect().
				ConnectTimeout(cfg.timeout).
				Transport(gorsocket.TCPClient().SetAddr(addr).Build()).
				Start(ctx)
			for id := range jobs {
				if err := uploadOnce(ctx, client2, cfg, id); err != nil {
					select {
					case errCh <- err:
						cancel()
					default:
					}
					return
				}
				stats.uploads.Add(1)
				stats.bytes.Add(uint64(cfg.blockSize * cfg.chunks))
			}
		}()
	}

sendJobs:
	for i := 0; i < cfg.requests; i++ {
		select {
		case jobs <- i:
		case <-ctx.Done():
			break sendJobs
		}
	}
	close(jobs)
	wg.Wait()

	select {
	case err := <-errCh:
		return err
	default:
	}
	if err := ctx.Err(); err != nil && stats.uploads.Load() != uint64(cfg.requests) {
		return err
	}
	return nil
}

func uploadOnce(ctx context.Context, client gorsocket.Client, cfg config, id int) error {
	startData, err := encodeStartPayload(cfg, id)
	if err != nil {
		return err
	}

	send := flux.Create(func(ctx context.Context, sink flux.Sink) {
		sink.Next(rspayload.New(startData, []byte(mossrsocket.StartPutObject)))
		for chunk := 0; chunk < cfg.chunks; chunk++ {
			if err := ctx.Err(); err != nil {
				sink.Error(err)
				return
			}
			data := makePattern(cfg.blockSize, uint64(id*cfg.chunks+chunk))
			sink.Next(rspayload.New(data, []byte(mossrsocket.PutObject)))
		}
		sink.Next(rspayload.New(nil, []byte(mossrsocket.CompletePutObject)))
		sink.Complete()
	})

	responses, err := client.RequestChannel(send).BlockSlice(ctx)
	if err != nil {
		return fmt.Errorf("upload %d request channel: %w", id, err)
	}
	for _, resp := range responses {
		meta, ok := resp.MetadataUTF8()
		if !ok {
			return fmt.Errorf("upload %d response missing metadata", id)
		}
		if mossrsocket.PayloadMetaType(meta) == mossrsocket.Error {
			return fmt.Errorf("upload %d server error: %s", id, string(resp.Data()))
		}
	}

	want := cfg.chunks + 1
	if len(responses) != want {
		return fmt.Errorf("upload %d got %d responses, want %d", id, len(responses), want)
	}
	for i := 0; i < cfg.chunks; i++ {
		meta, _ := responses[i].MetadataUTF8()
		if mossrsocket.PayloadMetaType(meta) != mossrsocket.Continue {
			return fmt.Errorf("upload %d response %d = %s, want %s", id, i, meta, mossrsocket.Continue)
		}
	}
	meta, _ := responses[len(responses)-1].MetadataUTF8()
	if mossrsocket.PayloadMetaType(meta) != mossrsocket.Success {
		return fmt.Errorf("upload %d final response = %s, want %s", id, meta, mossrsocket.Success)
	}
	return nil
}

func encodeStartPayload(cfg config, id int) ([]byte, error) {
	data := map[string]string{
		"lun":      cfg.lun,
		"fileName": fmt.Sprintf("%s-%d.bin", cfg.filePrefix, id),
		"metaKey":  fmt.Sprintf("%s-%d-%d", cfg.metaPrefix, time.Now().UnixNano(), id),
	}
	return json.Marshal(data)
}

func makePattern(size int, seed uint64) []byte {
	data := make([]byte, size)
	x := seed*0x9e3779b97f4a7c15 + 0xd1b54a32d192ed03
	for i := range data {
		x ^= x >> 12
		x ^= x << 25
		x ^= x >> 27
		data[i] = byte((x * 0x2545f4914f6cdd1d) >> 56)
	}
	return data
}

func parseSize(input string) (int64, error) {
	s := strings.TrimSpace(strings.ToLower(input))
	if s == "" {
		return 0, errors.New("empty size")
	}

	multiplier := float64(1)
	for suffix, mul := range map[string]float64{
		"kb": 1000,
		"mb": 1000 * 1000,
		"gb": 1000 * 1000 * 1000,
		"k":  1024,
		"m":  1024 * 1024,
		"g":  1024 * 1024 * 1024,
	} {
		if strings.HasSuffix(s, suffix) {
			multiplier = mul
			s = strings.TrimSpace(strings.TrimSuffix(s, suffix))
			break
		}
	}

	value, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, err
	}
	if value <= 0 || math.IsNaN(value) || math.IsInf(value, 0) {
		return 0, fmt.Errorf("invalid size %q", input)
	}
	return int64(value * multiplier), nil
}

func formatBytes(v uint64) string {
	const unit = 1024
	if v < unit {
		return fmt.Sprintf("%dB", v)
	}
	div := float64(unit)
	exp := 0
	value := float64(v)
	for value >= div*unit && exp < 5 {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.2f%ciB", value/div, "KMGTPE"[exp])
}
