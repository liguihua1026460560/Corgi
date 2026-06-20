//go:build linux
// +build linux

// io_bench: 对比 Go 原生同步阻塞 IO 与 io_uring 异步 IO 的性能。
//
// 编译:
//
//	go build -mod=vendor -o io_bench ./cmd/io_uring_test/
//
// 使用示例:
//
//	./io_bench -mode=both -bs=4k  -c=8  -d=30s
//	./io_bench -mode=both -bs=1m  -c=16 -d=30s -direct
//	./io_bench -mode=iouring -bs=4k -c=32 -qdepth=512 -d=60s -op=randread
//	./io_bench -mode=goaio -bs=1m -c=32 -qdepth=255 -d=60s -op=randwrite -direct
package main

import (
	"flag"
	"fmt"
	"math/rand/v2"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"unsafe"

	iouring "github.com/iceber/iouring-go"
	"golang.org/x/sys/unix"

	goaio "Corgi/internal/third_party/goaio"
)

// ---------- CLI flags ----------

var (
	flagMode     = flag.String("mode", "both", "bench mode: sync | iouring | goaio | both | all")
	flagBS       = flag.String("bs", "4k", "block size: 4k | 1m | <n>[k|m|g] e.g. 128k")
	flagConcur   = flag.Int("c", 8, "concurrent workers")
	flagDuration = flag.Duration("d", 30*time.Second, "duration per phase")
	flagFile     = flag.String("file", "/tmp/io_bench_data", "path to test data file")
	flagFileSize = flag.Int64("filesize", 512<<20, "test file size in bytes (default 512 MiB)")
	flagOp       = flag.String("op", "randread", "operation: randread | randwrite | read | write")
	flagQDepth   = flag.Int("qdepth", 256, "async submission queue depth")
	flagNoPrep   = flag.Bool("noprep", false, "skip test file preparation (file must exist)")
	flagDirect   = flag.Bool("direct", false, "open with O_DIRECT (bypass page cache; requires aligned IO)")
)

// ---------- helpers ----------

func parseBS(s string) int {
	s = strings.TrimSpace(strings.ToLower(s))
	mul := 1
	switch {
	case strings.HasSuffix(s, "g"):
		mul = 1 << 30
		s = s[:len(s)-1]
	case strings.HasSuffix(s, "m"):
		mul = 1 << 20
		s = s[:len(s)-1]
	case strings.HasSuffix(s, "k"):
		mul = 1 << 10
		s = s[:len(s)-1]
	}
	n, err := strconv.Atoi(s)
	if err != nil || n <= 0 {
		fmt.Fprintf(os.Stderr, "invalid block size %q\n", s)
		os.Exit(1)
	}
	return n * mul
}

func openFlags(write, direct bool) int {
	flags := os.O_RDONLY
	if write {
		flags = os.O_RDWR
	}
	if direct {
		flags |= syscall.O_DIRECT
		//flags |= syscall.O_SYNC // Ensure data is flushed to disk for more accurate benchmarking; may be redundant with O_DIRECT but adds safety.
	}
	return flags
}

// isBlockDevice returns true when path refers to a block special device.
// Used to refuse prepFile() on raw disks.
func isBlockDevice(path string) bool {
	fi, err := os.Stat(path)
	if err != nil {
		return false
	}
	return fi.Mode()&os.ModeDevice != 0
}

// tvDur converts syscall.Timeval to time.Duration.
func tvDur(tv syscall.Timeval) time.Duration {
	return time.Duration(tv.Sec)*time.Second + time.Duration(tv.Usec)*time.Microsecond
}

// ---------- stats ----------

type counter struct {
	ops    atomic.Int64
	bytes  atomic.Int64
	errors atomic.Int64
}

func (c *counter) snapshot() (ops, bytes, errors int64) {
	return c.ops.Load(), c.bytes.Load(), c.errors.Load()
}

// startReporter launches a goroutine that prints comprehensive stats every 5 s.
// Closes the returned channel when it exits (triggered by closing stop).
func startReporter(label string, c *counter, stop <-chan struct{}) chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		tick := time.NewTicker(5 * time.Second)
		defer tick.Stop()

		var prevOps, prevBytes int64
		prevWall := time.Now()
		var prevRu syscall.Rusage
		_ = syscall.Getrusage(syscall.RUSAGE_SELF, &prevRu)

		header := fmt.Sprintf("%-8s", label)

		print := func(now time.Time) {
			ops, bytes, errs := c.snapshot()
			elapsed := now.Sub(prevWall).Seconds()
			if elapsed < 1e-6 {
				elapsed = 1e-6
			}
			dOps := ops - prevOps
			dBytes := bytes - prevBytes

			iops := float64(dOps) / elapsed
			bw := float64(dBytes) / elapsed / (1 << 20)

			var ru syscall.Rusage
			_ = syscall.Getrusage(syscall.RUSAGE_SELF, &ru)
			uDelta := tvDur(ru.Utime) - tvDur(prevRu.Utime)
			sDelta := tvDur(ru.Stime) - tvDur(prevRu.Stime)
			cpuPct := (uDelta + sDelta).Seconds() / elapsed * 100

			var ms runtime.MemStats
			runtime.ReadMemStats(&ms)

			fmt.Printf(
				"[%s %s] IOPS=%8.0f  BW=%8.2f MiB/s  Err=%-4d  "+
					"CPU=%5.1f%%  Heap=%6.1f MiB  Sys=%6.1f MiB  "+
					"Goroutine=%-4d  NumGC=%-4d  GCPause=%7.2f ms  NextGC=%6.1f MiB\n",
				header, now.Format("15:04:05"),
				iops, bw, errs,
				cpuPct,
				float64(ms.HeapAlloc)/(1<<20),
				float64(ms.HeapSys)/(1<<20),
				runtime.NumGoroutine(),
				ms.NumGC,
				float64(ms.PauseTotalNs)/1e6,
				float64(ms.NextGC)/(1<<20),
			)

			prevOps = ops
			prevBytes = bytes
			prevWall = now
			prevRu = ru
		}

		for {
			select {
			case <-stop:
				print(time.Now())
				return
			case t := <-tick.C:
				print(t)
			}
		}
	}()
	return done
}

// ---------- test file preparation ----------

func prepFile(path string, size int64) {
	fmt.Printf("Preparing test file: %s (%d MiB)...\n", path, size>>20)
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		fmt.Fprintln(os.Stderr, "prepFile:", err)
		os.Exit(1)
	}
	defer f.Close()

	buf := make([]byte, 1<<20) // 1 MiB write chunks
	for i := range buf {
		buf[i] = byte(i & 0xFF)
	}
	written := int64(0)
	for written < size {
		chunk := size - written
		if chunk > int64(len(buf)) {
			chunk = int64(len(buf))
		}
		n, err := f.Write(buf[:chunk])
		if err != nil {
			fmt.Fprintln(os.Stderr, "prepFile write:", err)
			os.Exit(1)
		}
		written += int64(n)
	}
	if err := f.Sync(); err != nil {
		fmt.Fprintln(os.Stderr, "prepFile sync:", err)
		os.Exit(1)
	}
	fmt.Printf("File ready: %d bytes written.\n\n", written)
}

// ---------- sync IO benchmark ----------

func syncWorker(
	file *os.File,
	workerID int,
	bs int, fileBlocks int64,
	isWrite, isRandom bool,
	c *counter,
	stop <-chan struct{},
	wg *sync.WaitGroup,
) {
	defer wg.Done()
	// O_DIRECT requires 512-byte-aligned buffers; use 4 KiB alignment to be safe.
	buf := alignedBuffer(bs, 4096)
	if isWrite {
		for i := range buf {
			buf[i] = byte(i & 0xFF)
		}
	}
	// Sequential workers stagger their start so they don't all hammer the same block.
	seqOffset := int64(workerID) * int64(bs)
	for {
		select {
		case <-stop:
			return
		default:
		}
		var offset int64
		if isRandom {
			offset = rand.Int64N(fileBlocks) * int64(bs)
		} else {
			offset = seqOffset
			seqOffset += int64(bs)
			if seqOffset >= fileBlocks*int64(bs) {
				seqOffset = 0
			}
		}
		var n int
		var err error
		if isWrite {
			n, err = file.WriteAt(buf, offset)
		} else {
			n, err = file.ReadAt(buf, offset)
		}
		if err != nil {
			c.errors.Add(1)
			continue
		}
		c.ops.Add(1)
		c.bytes.Add(int64(n))
	}
}

func runSync(
	path string, fileSize int64,
	bs, concur int,
	dur time.Duration,
	op string, direct bool,
) (totalOps, totalBytes, totalErrors int64) {
	isWrite := strings.Contains(op, "write")
	isRandom := strings.HasPrefix(op, "rand") // randread/randwrite → random; read/write → sequential
	f, _ := unix.Open(path, unix.O_RDWR|unix.O_DIRECT|unix.O_CLOEXEC, 0666)
	file := os.NewFile(uintptr(f), path)
	defer file.Close()

	fileBlocks := fileSize / int64(bs)
	if fileBlocks < 1 {
		fileBlocks = 1
	}

	c := &counter{}
	stop := make(chan struct{})
	repDone := startReporter("sync", c, stop)

	var wg sync.WaitGroup
	for i := 0; i < concur; i++ {
		wg.Add(1)
		go syncWorker(file, i, bs, fileBlocks, isWrite, isRandom, c, stop, &wg)
	}

	time.Sleep(dur)
	close(stop)
	wg.Wait()
	<-repDone

	return c.snapshot()
}

// ---------- io_uring benchmark ----------

func iouringWorker(
	ring *iouring.IOURing,
	fd int,
	workerID int,
	bs int, fileBlocks int64,
	isWrite, isRandom bool,
	c *counter,
	stop <-chan struct{},
	wg *sync.WaitGroup,
) {
	defer wg.Done()
	// O_DIRECT requires 512-byte-aligned buffers; use 4 KiB alignment to be safe.
	buf := alignedBuffer(bs, 4096)
	if isWrite {
		for i := range buf {
			buf[i] = byte(i & 0xFF)
		}
	}
	ch := make(chan iouring.Result, 1)
	seqOffset := int64(workerID) * int64(bs)

	for {
		select {
		case <-stop:
			return
		default:
		}

		var rawOffset int64
		if isRandom {
			rawOffset = rand.Int64N(fileBlocks) * int64(bs)
		} else {
			rawOffset = seqOffset
			seqOffset += int64(bs)
			if seqOffset >= fileBlocks*int64(bs) {
				seqOffset = 0
			}
		}
		offset := uint64(rawOffset)

		var prep iouring.PrepRequest
		if isWrite {
			prep = iouring.Pwrite(fd, buf, offset)
		} else {
			prep = iouring.Pread(fd, buf, offset)
		}

		_, err := ring.SubmitRequest(prep, ch)
		if err != nil {
			c.errors.Add(1)
			continue
		}

		result := <-ch
		n, err := result.ReturnInt()
		if err != nil {
			c.errors.Add(1)
			continue
		}
		c.ops.Add(1)
		c.bytes.Add(int64(n))
	}
}

func runIOUring(
	path string, fileSize int64,
	bs, concur, qDepth int,
	dur time.Duration,
	op string, direct bool,
) (totalOps, totalBytes, totalErrors int64) {
	isWrite := strings.Contains(op, "write")
	isRandom := strings.HasPrefix(op, "rand")
	file, err := os.OpenFile(path, openFlags(isWrite, direct), 0644)
	if err != nil {
		fmt.Fprintln(os.Stderr, "runIOUring open:", err)
		os.Exit(1)
	}
	defer file.Close()

	fd := int(file.Fd())

	fileBlocks := fileSize / int64(bs)
	if fileBlocks < 1 {
		fileBlocks = 1
	}

	// Queue depth must be >= concurrency to avoid "ring full" errors.
	if qDepth < concur*2 {
		qDepth = concur * 2
	}

	ring, err := iouring.New(uint(qDepth))
	if err != nil {
		fmt.Fprintln(os.Stderr, "iouring.New:", err)
		os.Exit(1)
	}
	defer ring.Close()

	c := &counter{}
	stop := make(chan struct{})
	repDone := startReporter("iouring", c, stop)

	var wg sync.WaitGroup
	for i := 0; i < concur; i++ {
		wg.Add(1)
		go iouringWorker(ring, fd, i, bs, fileBlocks, isWrite, isRandom, c, stop, &wg)
	}

	time.Sleep(dur)
	close(stop)
	wg.Wait()
	<-repDone

	return c.snapshot()
}

// ---------- goaio benchmark ----------

func goaioWorker(
	a *goaio.AIO,
	workerID int,
	bs int, fileBlocks int64,
	isWrite, isRandom bool,
	c *counter,
	stop <-chan struct{},
	wg *sync.WaitGroup,
) {
	defer wg.Done()
	buf := alignedBuffer(bs, 4096)
	if isWrite {
		for i := range buf {
			buf[i] = byte(i & 0xFF)
		}
	}
	seqOffset := int64(workerID) * int64(bs)

	for {
		select {
		case <-stop:
			return
		default:
		}

		var offset int64
		if isRandom {
			offset = rand.Int64N(fileBlocks) * int64(bs)
		} else {
			offset = seqOffset
			seqOffset += int64(bs)
			if seqOffset >= fileBlocks*int64(bs) {
				seqOffset = 0
			}
		}
		var (
			id  goaio.RequestId
			err error
		)
		if isWrite {
			id, err = a.WriteAt(buf, offset)
		} else {
			id, err = a.ReadAt(buf, offset)
		}
		if err != nil {
			c.errors.Add(1)
			continue
		}

		n, err := a.WaitFor(id)

		if err != nil {
			c.errors.Add(1)
			continue
		}
		c.ops.Add(1)
		c.bytes.Add(int64(n))
	}
}

func runGoAIO(
	path string, fileSize int64,
	bs, concur, qDepth int,
	dur time.Duration,
	op string, direct bool,
) (totalOps, totalBytes, totalErrors int64) {
	isWrite := strings.Contains(op, "write")
	isRandom := strings.HasPrefix(op, "rand")
	flags := openFlags(isWrite, direct)
	if isWrite {
		flags |= os.O_CREATE
	}

	a, err := goaio.NewAIOExt(path, goaio.AIOExtConfig{QueueDepth: qDepth}, flags, 0644)
	if err == goaio.ErrInvalidQueueDepth && qDepth >= 256 {
		qDepth = 255
		a, err = goaio.NewAIOExt(path, goaio.AIOExtConfig{QueueDepth: qDepth}, flags, 0644)
		fmt.Printf("  [WARN] goaio queue depth capped to %d by current goaio limits.\n", qDepth)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "goaio.NewAIOExt:", err)
		os.Exit(1)
	}
	defer a.Close()

	fileBlocks := fileSize / int64(bs)
	if fileBlocks < 1 {
		fileBlocks = 1
	}

	c := &counter{}
	stop := make(chan struct{})
	repDone := startReporter("goaio", c, stop)

	var wg sync.WaitGroup
	for i := 0; i < concur; i++ {
		wg.Add(1)
		go goaioWorker(a, i, bs, fileBlocks, isWrite, isRandom, c, stop, &wg)
	}

	time.Sleep(dur)
	close(stop)
	wg.Wait()
	<-repDone

	return c.snapshot()
}

func alignedBuffer(size, alignment int) []byte {
	if size <= 0 {
		return nil
	}
	raw := make([]byte, size+alignment-1)
	base := uintptr(unsafe.Pointer(&raw[0]))
	start := 0
	if rem := base % uintptr(alignment); rem != 0 {
		start = int(uintptr(alignment) - rem)
	}
	return raw[start : start+size]
}

// ---------- summary ----------

func printSummary(label string, ops, bytes, errors int64, dur time.Duration) {
	elapsed := dur.Seconds()
	fmt.Printf("  %-8s IOPS=%8.0f  BW=%8.2f MiB/s  Total=%8.2f MiB  Errors=%d\n",
		label,
		float64(ops)/elapsed,
		float64(bytes)/elapsed/(1<<20),
		float64(bytes)/(1<<20),
		errors,
	)
}

// ---------- main ----------

func main() {
	flag.Parse()

	bs := parseBS(*flagBS)

	fmt.Printf("╔══════════════════════════════════════════════════════╗\n")
	fmt.Printf("║               IO Bench (sync / io_uring / goaio)     ║\n")
	fmt.Printf("╚══════════════════════════════════════════════════════╝\n")
	fmt.Printf("  Mode        : %s\n", *flagMode)
	fmt.Printf("  Operation   : %s\n", *flagOp)
	fmt.Printf("  Block size  : %s (%d bytes)\n", *flagBS, bs)
	fmt.Printf("  Concurrency : %d workers\n", *flagConcur)
	fmt.Printf("  Duration    : %s per phase\n", *flagDuration)
	fmt.Printf("  File        : %s\n", *flagFile)
	fmt.Printf("  File size   : %d MiB\n", *flagFileSize>>20)
	fmt.Printf("  Queue depth : %d  (async)\n", *flagQDepth)
	fmt.Printf("  O_DIRECT    : %v\n", *flagDirect)
	if *flagDirect {
		fmt.Printf("  [WARN] O_DIRECT requires block-aligned buffers; test may error on some FS setups.\n")
	}
	fmt.Println()

	if !*flagNoPrep {
		// Refuse to overwrite raw block devices: prepFile uses O_TRUNC which
		// would destroy partition data. The caller must pass -noprep explicitly.
		if isBlockDevice(*flagFile) {
			fmt.Fprintf(os.Stderr,
				"[ERROR] %q is a raw block device. Refusing to run prepFile() on it.\n"+
					"       Add -noprep to skip file preparation when testing block devices.\n",
				*flagFile)
			os.Exit(1)
		}
		prepFile(*flagFile, *flagFileSize)
	}

	var (
		syncOps, syncBytes, syncErrors    int64
		iourOps, iourBytes, iourErrors    int64
		goaioOps, goaioBytes, goaioErrors int64
		hasSyncResult, hasIouringResult   bool
		hasGoAIOResult                    bool
	)

	if *flagMode == "sync" || *flagMode == "both" || *flagMode == "all" {
		fmt.Printf("━━━ Phase: sync IO (%s) ━━━\n", *flagOp)
		syncOps, syncBytes, syncErrors = runSync(
			*flagFile, *flagFileSize, bs, *flagConcur, *flagDuration, *flagOp, *flagDirect,
		)
		hasSyncResult = true
		fmt.Println()
	}

	if *flagMode == "iouring" || *flagMode == "both" || *flagMode == "all" {
		fmt.Printf("━━━ Phase: io_uring IO (%s) ━━━\n", *flagOp)
		iourOps, iourBytes, iourErrors = runIOUring(
			*flagFile, *flagFileSize, bs, *flagConcur, *flagQDepth, *flagDuration, *flagOp, *flagDirect,
		)
		hasIouringResult = true
		fmt.Println()
	}

	if *flagMode == "goaio" || *flagMode == "all" {
		fmt.Printf("━━━ Phase: goaio IO (%s) ━━━\n", *flagOp)
		goaioOps, goaioBytes, goaioErrors = runGoAIO(
			*flagFile, *flagFileSize, bs, *flagConcur, *flagQDepth, *flagDuration, *flagOp, *flagDirect,
		)
		hasGoAIOResult = true
		fmt.Println()
	}

	fmt.Printf("━━━ Summary (duration=%s, bs=%s, concur=%d) ━━━\n",
		*flagDuration, *flagBS, *flagConcur)
	if hasSyncResult {
		printSummary("sync", syncOps, syncBytes, syncErrors, *flagDuration)
	}
	if hasIouringResult {
		printSummary("iouring", iourOps, iourBytes, iourErrors, *flagDuration)
	}
	if hasGoAIOResult {
		printSummary("goaio", goaioOps, goaioBytes, goaioErrors, *flagDuration)
	}
	if hasSyncResult && hasIouringResult && syncOps > 0 {
		fmt.Printf("\n  io_uring vs sync speedup:\n")
		fmt.Printf("    IOPS:      %.2fx\n", float64(iourOps)/float64(syncOps))
		fmt.Printf("    Bandwidth: %.2fx\n", float64(iourBytes)/float64(syncBytes))
	}
	if hasSyncResult && hasGoAIOResult && syncOps > 0 {
		fmt.Printf("\n  goaio vs sync speedup:\n")
		fmt.Printf("    IOPS:      %.2fx\n", float64(goaioOps)/float64(syncOps))
		fmt.Printf("    Bandwidth: %.2fx\n", float64(goaioBytes)/float64(syncBytes))
	}
}
