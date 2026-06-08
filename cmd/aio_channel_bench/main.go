package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"mossserver/internal/com/macrosan/fs"
)

const defaultStatsInterval = time.Second

type config struct {
	mountDir        string
	lun             string
	caseName        string
	concurrency     int
	readConcurrency int
	blockSize       int64
	totalBytes      int64
	resultSize      int64
	statsInterval   time.Duration
	timeout         time.Duration
	verify          bool
	clockTicks      int64
}

type counters struct {
	writeBytes atomic.Uint64
	writeOps   atomic.Uint64
	readBytes  atomic.Uint64
	readOps    atomic.Uint64
	errors     atomic.Uint64
}

type writeRecord struct {
	results []fs.Result
	token   []fs.Result
	data    []byte
	length  int
}

type caseFunc func(context.Context, *fs.BlockDevice, config, *counters) error

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

	device, err := openDevice(ctx, cfg)
	if err != nil {
		log.Fatal(err)
	}
	defer closeDevices()

	var stats counters
	stopReporter := startReporter(ctx, cfg, &stats)
	startMem := readMem()
	start := time.Now()

	err = runSelectedCase(ctx, device, cfg, &stats)

	elapsed := time.Since(start)
	stopReporter()
	printSummary(elapsed, startMem, readMem(), &stats)

	if err != nil {
		log.Fatal(err)
	}
}

func parseConfig(args []string) (config, error) {
	cfg := config{
		caseName:        "mixed-rw",
		concurrency:     runtime.NumCPU(),
		readConcurrency: runtime.NumCPU(),
		statsInterval:   defaultStatsInterval,
		verify:          true,
		clockTicks:      100,
	}

	blockSize := "1m"
	totalBytes := "1g"
	resultSize := "64k"

	fsFlags := flag.NewFlagSet("aio_channel_bench", flag.ContinueOnError)
	fsFlags.StringVar(&cfg.mountDir, "mount", "", "block device mount dir; positional first arg is also accepted")
	fsFlags.StringVar(&cfg.lun, "lun", "", "lun name to test; empty selects the first initialized device")
	fsFlags.StringVar(&cfg.caseName, "case", cfg.caseName, "test case: small, multi-result, partial-last, mixed-rw, all")
	fsFlags.IntVar(&cfg.concurrency, "concurrency", cfg.concurrency, "write concurrency")
	fsFlags.IntVar(&cfg.readConcurrency, "read-concurrency", cfg.readConcurrency, "read concurrency")
	fsFlags.StringVar(&blockSize, "block-size", blockSize, "bytes per logical write, e.g. 4k, 1m, 64m")
	fsFlags.StringVar(&totalBytes, "total", totalBytes, "total logical write bytes, e.g. 1g")
	fsFlags.StringVar(&resultSize, "result-size", resultSize, "manual Result split size for multi-result cases")
	fsFlags.DurationVar(&cfg.statsInterval, "stats-interval", cfg.statsInterval, "stats print interval")
	fsFlags.DurationVar(&cfg.timeout, "timeout", 0, "optional timeout")
	fsFlags.BoolVar(&cfg.verify, "verify", cfg.verify, "read back and verify written bytes")
	fsFlags.Int64Var(&cfg.clockTicks, "clock-ticks", cfg.clockTicks, "Linux clock ticks per second for /proc/self/stat CPU calculation")
	if err := fsFlags.Parse(args); err != nil {
		return config{}, err
	}

	if cfg.mountDir == "" && fsFlags.NArg() > 0 {
		cfg.mountDir = fsFlags.Arg(0)
	}
	if cfg.mountDir == "" {
		return config{}, errors.New("missing -mount <dir>")
	}
	if cfg.concurrency <= 0 {
		return config{}, errors.New("-concurrency must be > 0")
	}
	if cfg.readConcurrency <= 0 {
		return config{}, errors.New("-read-concurrency must be > 0")
	}
	if cfg.statsInterval <= 0 {
		return config{}, errors.New("-stats-interval must be > 0")
	}
	if cfg.clockTicks <= 0 {
		return config{}, errors.New("-clock-ticks must be > 0")
	}

	var err error
	cfg.blockSize, err = parseSize(blockSize)
	if err != nil {
		return config{}, fmt.Errorf("invalid -block-size: %w", err)
	}
	cfg.totalBytes, err = parseSize(totalBytes)
	if err != nil {
		return config{}, fmt.Errorf("invalid -total: %w", err)
	}
	cfg.resultSize, err = parseSize(resultSize)
	if err != nil {
		return config{}, fmt.Errorf("invalid -result-size: %w", err)
	}
	if cfg.blockSize <= 0 || cfg.totalBytes <= 0 || cfg.resultSize <= 0 {
		return config{}, errors.New("sizes must be > 0")
	}

	cfg.caseName = strings.ToLower(strings.TrimSpace(cfg.caseName))
	return cfg, nil
}

func openDevice(ctx context.Context, cfg config) (*fs.BlockDevice, error) {
	if err := fs.Init(ctx, cfg.mountDir); err != nil {
		return nil, err
	}

	devices := fs.Devices()
	if len(devices) == 0 {
		return nil, fmt.Errorf("no block device found under mount dir %q", cfg.mountDir)
	}

	if cfg.lun != "" {
		device := fs.Get(cfg.lun)
		if device == nil {
			return nil, fmt.Errorf("lun %q not found", cfg.lun)
		}
		return device, nil
	}

	names := make([]string, 0, len(devices))
	for name := range devices {
		names = append(names, name)
	}
	sort.Strings(names)
	return devices[names[0]], nil
}

func closeDevices() {
	for name := range fs.Devices() {
		if err := fs.Remove(name); err != nil {
			log.Printf("close device %s: %v", name, err)
		}
	}
}

func runSelectedCase(ctx context.Context, device *fs.BlockDevice, cfg config, stats *counters) error {
	log.Printf("device=%s path=%s size=%s case=%s writeConcurrency=%d readConcurrency=%d blockSize=%s total=%s verify=%t",
		device.Name(), device.Path(), formatBytes(uint64(device.Size())), cfg.caseName,
		cfg.concurrency, cfg.readConcurrency, formatBytes(uint64(cfg.blockSize)),
		formatBytes(uint64(cfg.totalBytes)), cfg.verify)

	cases := map[string]caseFunc{
		"small":        runSmallCase,
		"multi-result": runMultiResultCase,
		"partial-last": runPartialLastCase,
		"mixed-rw":     runMixedRWCase,
	}

	if cfg.caseName == "all" {
		if err := runSmallCase(ctx, device, cfg, stats); err != nil {
			return fmt.Errorf("small: %w", err)
		}
		if err := runMultiResultCase(ctx, device, cfg, stats); err != nil {
			return fmt.Errorf("multi-result: %w", err)
		}
		if err := runPartialLastCase(ctx, device, cfg, stats); err != nil {
			return fmt.Errorf("partial-last: %w", err)
		}
		return runMixedRWCase(ctx, device, cfg, stats)
	}

	fn, ok := cases[cfg.caseName]
	if !ok {
		return fmt.Errorf("unknown -case %q; choose small, multi-result, partial-last, mixed-rw, all", cfg.caseName)
	}
	return fn(ctx, device, cfg, stats)
}

func runSmallCase(ctx context.Context, device *fs.BlockDevice, cfg config, stats *counters) error {
	small := cfg
	// if small.blockSize > fs.BlockSize {
	// 	small.blockSize = fs.BlockSize
	// }
	log.Printf("case small: concurrent small write/read-back, blockSize=%s", formatBytes(uint64(small.blockSize)))
	return runWriteReadPipeline(ctx, device, small, stats, func(op uint64, size int) ([]fs.Result, []fs.Result, []byte, error) {
		data := makePattern(size, op)
		results, token, err := device.Channel.Write(ctx, data)
		if err != nil {
			return nil, nil, nil, err
		}
		return results, token, data, nil
	})
}

func runMixedRWCase(ctx context.Context, device *fs.BlockDevice, cfg config, stats *counters) error {
	log.Printf("case mixed-rw: concurrent write/read-back throughput")
	return runWriteReadPipeline(ctx, device, cfg, stats, func(op uint64, size int) ([]fs.Result, []fs.Result, []byte, error) {
		data := makePattern(size, op)
		results, token, err := device.Channel.Write(ctx, data)
		if err != nil {
			return nil, nil, nil, err
		}
		return results, token, data, nil
	})
}

func runMultiResultCase(ctx context.Context, device *fs.BlockDevice, cfg config, stats *counters) error {
	log.Printf("case multi-result: WriteAllocated over manually split Results")
	splitSize := cfg.resultSize
	if splitSize >= cfg.blockSize {
		splitSize = maxInt64(1, cfg.blockSize/3)
	}
	return runWriteReadPipeline(ctx, device, cfg, stats, func(op uint64, size int) ([]fs.Result, []fs.Result, []byte, error) {
		data := makePattern(size, op)
		results, token, err := device.Alloc(int64(size))
		if err != nil {
			return nil, nil, nil, err
		}
		split := splitResults(results, splitSize)
		written, err := device.Channel.WriteAllocated(ctx, data, split)
		if err != nil {
			device.RollbackAlloc(token)
			return nil, nil, nil, err
		}
		if written != len(data) {
			device.RollbackAlloc(token)
			return nil, nil, nil, io.ErrShortWrite
		}
		return split, token, data, nil
	})
}

func runPartialLastCase(ctx context.Context, device *fs.BlockDevice, cfg config, stats *counters) error {
	log.Printf("case partial-last: allocated size is larger than data length; tail must stay unchanged")
	return runWriteReadPipeline(ctx, device, cfg, stats, func(op uint64, size int) ([]fs.Result, []fs.Result, []byte, error) {
		data := makePattern(size, op)
		allocSize := fitBlock(int64(size)) + fs.BlockSize
		results, token, err := device.Alloc(allocSize)
		if err != nil {
			return nil, nil, nil, err
		}
		split := splitResults(results, minInt64(cfg.resultSize, fs.BlockSize))

		marker := bytes.Repeat([]byte{0xee}, int(allocSize))
		written, err := device.Channel.WriteAllocated(ctx, marker, split)
		if err != nil {
			device.RollbackAlloc(token)
			return nil, nil, nil, err
		}
		if written != len(marker) {
			device.RollbackAlloc(token)
			return nil, nil, nil, io.ErrShortWrite
		}
		stats.writeBytes.Add(uint64(len(marker)))
		stats.writeOps.Add(1)

		written, err = device.Channel.WriteAllocated(ctx, data, split)
		if err != nil {
			device.RollbackAlloc(token)
			return nil, nil, nil, err
		}
		if written != len(data) {
			device.RollbackAlloc(token)
			return nil, nil, nil, io.ErrShortWrite
		}

		want := append(data, marker[len(data):]...)
		return split, token, want, nil
	})
}

func runWriteReadPipeline(
	ctx context.Context,
	device *fs.BlockDevice,
	cfg config,
	stats *counters,
	writeOne func(op uint64, size int) ([]fs.Result, []fs.Result, []byte, error),
) error {
	ops := uint64(ceilDiv(cfg.totalBytes, cfg.blockSize))
	records := make(chan writeRecord, cfg.concurrency+cfg.readConcurrency)
	errCh := make(chan error, cfg.concurrency+cfg.readConcurrency)
	var nextOp atomic.Uint64
	var wgWriters sync.WaitGroup
	var wgReaders sync.WaitGroup

	for i := 0; i < cfg.readConcurrency; i++ {
		wgReaders.Add(1)
		go func() {
			defer wgReaders.Done()
			for rec := range records {
				if err := readAndVerify(ctx, device, cfg, rec, stats); err != nil {
					stats.errors.Add(1)
					select {
					case errCh <- err:
					default:
					}
				}
			}
		}()
	}

	for i := 0; i < cfg.concurrency; i++ {
		wgWriters.Add(1)
		go func() {
			defer wgWriters.Done()
			for {
				op := nextOp.Add(1) - 1
				if op >= ops {
					return
				}
				size := opSize(cfg.totalBytes, cfg.blockSize, op)
				results, token, data, err := writeOne(op, size)
				if err != nil {
					stats.errors.Add(1)
					select {
					case errCh <- err:
					default:
					}
					return
				}
				stats.writeBytes.Add(uint64(size))
				stats.writeOps.Add(1)

				select {
				case records <- writeRecord{results: results, token: token, data: data, length: len(data)}:
				case <-ctx.Done():
					device.RollbackAlloc(token)
					return
				}
			}
		}()
	}

	wgWriters.Wait()
	close(records)
	wgReaders.Wait()

	select {
	case err := <-errCh:
		return err
	default:
		return ctx.Err()
	}
}

func readAndVerify(ctx context.Context, device *fs.BlockDevice, cfg config, rec writeRecord, stats *counters) error {
	defer device.RollbackAlloc(rec.token)

	got, err := readResults(ctx, device.Channel, rec.results, rec.length)
	if err != nil {
		return err
	}
	stats.readBytes.Add(uint64(len(got)))
	stats.readOps.Add(1)

	if cfg.verify && !bytes.Equal(got, rec.data) {
		return fmt.Errorf("read verification failed: len=%d", rec.length)
	}
	return nil
}

func readResults(ctx context.Context, channel *fs.AioChannel, results []fs.Result, length int) ([]byte, error) {
	out := make([]byte, length)
	cursor := 0
	for _, r := range results {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if cursor >= length {
			break
		}
		need := int(r.Size)
		if left := length - cursor; need > left {
			need = left
		}
		part, err := channel.Read(ctx, r.Offset, need)
		if err != nil {
			return nil, err
		}
		copy(out[cursor:cursor+need], part)
		channel.PutReadBuffer(part)
		cursor += need
	}
	if cursor != length {
		return nil, io.ErrUnexpectedEOF
	}
	return out, nil
}

func splitResults(results []fs.Result, maxSize int64) []fs.Result {
	if maxSize <= 0 {
		return results
	}
	out := make([]fs.Result, 0, len(results))
	for _, r := range results {
		offset := r.Offset
		left := r.Size
		for left > 0 {
			n := maxSize
			if n > left {
				n = left
			}
			out = append(out, fs.Result{Offset: offset, Size: n})
			offset += n
			left -= n
		}
	}
	return out
}

func opSize(totalBytes, blockSize int64, op uint64) int {
	start := int64(op) * blockSize
	left := totalBytes - start
	if left <= 0 {
		return 0
	}
	if left > blockSize {
		left = blockSize
	}
	return int(left)
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

func startReporter(ctx context.Context, cfg config, stats *counters) func() {
	done := make(chan struct{})
	var once sync.Once
	last := sampleStats(cfg, stats)
	go func() {
		ticker := time.NewTicker(cfg.statsInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-done:
				return
			case <-ticker.C:
				cur := sampleStats(cfg, stats)
				printIntervalStats(last, cur)
				last = cur
			}
		}
	}()
	return func() {
		once.Do(func() {
			close(done)
		})
	}
}

type statSample struct {
	at         time.Time
	writeBytes uint64
	writeOps   uint64
	readBytes  uint64
	readOps    uint64
	errors     uint64
	cpuNanos   uint64
	mem        runtime.MemStats
}

func sampleStats(cfg config, stats *counters) statSample {
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	cpu, _ := processCPUNanos(cfg.clockTicks)
	return statSample{
		at:         time.Now(),
		writeBytes: stats.writeBytes.Load(),
		writeOps:   stats.writeOps.Load(),
		readBytes:  stats.readBytes.Load(),
		readOps:    stats.readOps.Load(),
		errors:     stats.errors.Load(),
		cpuNanos:   cpu,
		mem:        mem,
	}
}

func printIntervalStats(prev, cur statSample) {
	seconds := cur.at.Sub(prev.at).Seconds()
	if seconds <= 0 {
		return
	}
	writeBytes := cur.writeBytes - prev.writeBytes
	writeOps := cur.writeOps - prev.writeOps
	readBytes := cur.readBytes - prev.readBytes
	readOps := cur.readOps - prev.readOps
	cpuRaw := 0.0
	cpuMachine := 0.0
	if cur.cpuNanos >= prev.cpuNanos {
		cpuRaw = float64(cur.cpuNanos-prev.cpuNanos) / float64(cur.at.Sub(prev.at).Nanoseconds()) * 100
		cpuMachine = cpuRaw / float64(runtime.NumCPU())
	}
	totalAllocRate := float64(cur.mem.TotalAlloc-prev.mem.TotalAlloc) / seconds
	mallocRate := float64(cur.mem.Mallocs-prev.mem.Mallocs) / seconds
	freeRate := float64(cur.mem.Frees-prev.mem.Frees) / seconds
	gcDelta := cur.mem.NumGC - prev.mem.NumGC
	pauseDelta := cur.mem.PauseTotalNs - prev.mem.PauseTotalNs

	fmt.Printf(
		"[%s] write_bw=%s/s write_iops=%.0f read_bw=%s/s read_iops=%.0f cpu=%.1f%%(%.1f%%/%dcpu) alloc=%s total_alloc=%s/s mallocs=%.0f/s frees=%.0f/s heap=%s heap_objs=%d gc=%d pause=%s gccpu=%.4f errors=%d\n",
		cur.at.Format("15:04:05"),
		formatBytes(uint64(float64(writeBytes)/seconds)),
		float64(writeOps)/seconds,
		formatBytes(uint64(float64(readBytes)/seconds)),
		float64(readOps)/seconds,
		cpuRaw,
		cpuMachine,
		runtime.NumCPU(),
		formatBytes(cur.mem.Alloc),
		formatBytes(uint64(totalAllocRate)),
		mallocRate,
		freeRate,
		formatBytes(cur.mem.HeapAlloc),
		cur.mem.HeapObjects,
		gcDelta,
		time.Duration(pauseDelta),
		cur.mem.GCCPUFraction,
		cur.errors,
	)
}

func printSummary(elapsed time.Duration, startMem, endMem runtime.MemStats, stats *counters) {
	seconds := elapsed.Seconds()
	if seconds <= 0 {
		seconds = 1
	}
	writeOps := stats.writeOps.Load()
	readOps := stats.readOps.Load()
	ops := writeOps
	if ops == 0 {
		ops = 1
	}
	totalAlloc := endMem.TotalAlloc - startMem.TotalAlloc
	mallocs := endMem.Mallocs - startMem.Mallocs

	fmt.Printf(
		"summary elapsed=%s write=%s write_bw=%s/s write_iops=%.0f read=%s read_bw=%s/s read_iops=%.0f alloc/op=%s mallocs/op=%.2f gc=%d pause=%s errors=%d\n",
		elapsed.Round(time.Millisecond),
		formatBytes(stats.writeBytes.Load()),
		formatBytes(uint64(float64(stats.writeBytes.Load())/seconds)),
		float64(writeOps)/seconds,
		formatBytes(stats.readBytes.Load()),
		formatBytes(uint64(float64(stats.readBytes.Load())/seconds)),
		float64(readOps)/seconds,
		formatBytes(totalAlloc/ops),
		float64(mallocs)/float64(ops),
		endMem.NumGC-startMem.NumGC,
		time.Duration(endMem.PauseTotalNs-startMem.PauseTotalNs),
		stats.errors.Load(),
	)
}

func readMem() runtime.MemStats {
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	return mem
}

func processCPUNanos(clockTicks int64) (uint64, bool) {
	raw, err := os.ReadFile("/proc/self/stat")
	if err != nil {
		return 0, false
	}
	text := string(raw)
	endComm := strings.LastIndex(text, ")")
	if endComm < 0 || endComm+2 >= len(text) {
		return 0, false
	}
	fields := strings.Fields(text[endComm+2:])
	if len(fields) < 13 {
		return 0, false
	}
	utime, err1 := strconv.ParseUint(fields[11], 10, 64)
	stime, err2 := strconv.ParseUint(fields[12], 10, 64)
	if err1 != nil || err2 != nil {
		return 0, false
	}
	return (utime + stime) * uint64(time.Second) / uint64(clockTicks), true
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

func ceilDiv(a, b int64) int64 {
	if b <= 0 {
		return 0
	}
	return (a + b - 1) / b
}

func fitBlock(size int64) int64 {
	if size%fs.BlockSize == 0 {
		return size
	}
	return (size/fs.BlockSize + 1) * fs.BlockSize
}

func minInt64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
