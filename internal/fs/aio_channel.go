package fs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"unsafe"

	goaio "Corgi/internal/third_party/goaio"
)

const maxPoolBufSize = 4 * 1024 * 1024

// aioResult is the completion signal delivered per submitted request.
type aioResult struct {
	n   int
	err error
}

// AioChannel implements the Java aioThread model:
//
//   - Submit path: goroutines call aio.WriteAt concurrently.  goaio's own
//     dmtx serialises the kernel io_submit call and returns immediately after
//     the syscall returns.  No channel-level lock is held during or after
//     submission.
//
//   - Completion path: a single background goroutine (completionLoop) calls
//     aio.WaitAny in a tight loop and fans out results to per-request channels
//     stored in the inflightMap.
//
// This matches Java's design where write() only calls JNI io_submit and the
// aioThread background loop calls io_getevents and resolves futures.
type AioChannel struct {
	device *BlockDevice
	file   *os.File
	aio    *goaio.AIO

	// stopCh is closed by Close() to stop the completion loop.
	stopCh  chan struct{}
	stopped atomic.Bool
	loopWG  sync.WaitGroup

	readPool sync.Pool
}

func NewAioChannel(device *BlockDevice) (*AioChannel, error) {
	aio, err := goaio.New(device.file, goaio.AIOExtConfig{QueueDepth: 1024})
	if err != nil {
		return nil, err
	}

	c := &AioChannel{
		device: device,
		file:   device.file,
		aio:    aio,
		stopCh: make(chan struct{}),
	}
	c.readPool.New = func() any {
		buf := make([]byte, 0)
		return &buf
	}

	// Start the Java-equivalent aioThread: one goroutine draining completions.
	c.loopWG.Add(1)
	go c.completionLoop()

	return c, nil
}

// completionLoop is the Go equivalent of Java's background aioThread.
//
// It calls aio.WaitAny() (which invokes io_getevents) in a loop and fans out
// completed request IDs to the waiting goroutines via per-request channels.
// It does NOT hold any lock while blocking in WaitAny.
func (c *AioChannel) completionLoop() {
	defer c.loopWG.Done()
	for {
		select {
		case <-c.stopCh:
			return
		default:
		}
		_, err := c.aio.WaitAnyNotify()

		if err != nil {
			// 这里是 io_getevents 或内部状态错误。
			// 如果是磁盘致命错误，可以标记 device unhealthy。
			fmt.Printf("aio WaitAnyNotify error: %v\n", err)
		}
	}
}

// submitAndWait submits one segment to the kernel via aio.WriteAt and then
// waits for the completion loop to signal the result.
//
// goaio.WriteAt internally acquires dmtx only for the duration of the
// io_submit syscall, so multiple goroutines can submit concurrently; they
// serialise only at the kernel iocb-slot allocation level, not at the
// whole-write level.
func (c *AioChannel) submitAndWait(ctx context.Context, data []byte, offset int64) (int, error) {
	resCh := make(chan goaio.Completion, 1)

	if err := c.aio.WriteAtNotify(data, offset, resCh); err != nil {
		return 0, err
	}

	select {
	case <-ctx.Done():
		// 不要删除底层状态。
		// 请求已经提交给内核，后续 completionLoop 仍然要回收 cb/slot。
		return 0, ctx.Err()

	case res := <-resCh:
		return res.N, res.Err
	}
}

func (c *AioChannel) Write(ctx context.Context, data []byte) ([]Result, []Result, error) {
	alignedSize := AlignBlockSize(int64(len(data)))
	alignedData := NewAlignedBlockBuffer(int(alignedSize))
	copy(alignedData, data)

	results, token, err := c.device.Alloc(alignedSize)
	if err != nil {
		return nil, nil, err
	}
	if _, err = c.WriteAllocated(ctx, alignedData, results); err != nil {
		c.device.RollbackAlloc(token)
		return nil, nil, err
	}
	return results, token, nil
}

// WriteAllocated submits all segments concurrently (submit path is lock-free
// at the AioChannel level) and waits for all completions.
//
// Multiple goroutines can call WriteAllocated simultaneously.  The only
// serialisation is inside goaio for the kernel iocb-slot allocation
// (dmtx held for microseconds around io_submit), which matches Java's
// behaviour where multiple upload goroutines can co-submit to the same
// io_context.
func (c *AioChannel) WriteAllocated(ctx context.Context, data []byte, results []Result) (int, error) {
	if len(data) == 0 {
		return 0, nil
	}
	if c.aio == nil {
		return 0, errors.New("aio channel is not initialized")
	}
	if !isAlignedBuffer(data) {
		return 0, fmtAlignmentError("buffer", 0, int64(len(data)))
	}

	// Build the segment list first (no lock needed).
	type segment struct {
		data   []byte
		offset int64
	}
	segments := make([]segment, 0, len(results))
	cursor := 0
	for _, r := range results {
		if err := ctx.Err(); err != nil {
			return cursor, err
		}
		if cursor >= len(data) {
			break
		}
		if err := validateAlignedResult(r); err != nil {
			return cursor, err
		}
		need64 := r.Size
		if left := len(data) - cursor; int64(left) < need64 {
			need64 = int64(left)
		}
		need := int(need64)
		if need <= 0 || int64(need)%BlockSize != 0 {
			return cursor, fmtAlignmentError("len", r.Offset, int64(need))
		}
		segments = append(segments, segment{
			data:   data[cursor : cursor+need],
			offset: r.Offset,
		})
		cursor += need
	}
	if cursor != len(data) {
		return 0, io.ErrShortWrite
	}

	// Submit all segments and collect per-segment result channels.
	// submitAndWait is called concurrently-safe: each call acquires dmtx
	// only for the io_submit syscall duration, then registers the result
	// channel and returns.  We do this sequentially here because the
	// caller (runFlush goroutine) already represents one upload's concurrency
	// unit; spreading segments across goroutines would not improve throughput
	// and would complicate error handling.
	writtenTotal := 0
	for _, seg := range segments {
		if err := ctx.Err(); err != nil {
			return writtenTotal, err
		}
		n, err := c.submitAndWait(ctx, seg.data, seg.offset)
		writtenTotal += n
		if err != nil {
			fmt.Printf("aio WriteAllocated error: %v, writtenTotal=%d, len=%d, offset=%d", err, writtenTotal, len(seg.data), seg.offset)
			return writtenTotal, err
		}
		if n != len(seg.data) {
			return writtenTotal, io.ErrShortWrite
		}
	}
	return writtenTotal, nil
}

func (c *AioChannel) Read(ctx context.Context, offset int64, length int) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if length < 0 {
		return nil, errors.New("read length must be >= 0")
	}
	buf := c.getReadBuffer(length)
	n, err := c.file.ReadAt(buf, offset)
	if err != nil && !errors.Is(err, io.EOF) {
		c.PutReadBuffer(buf)
		return nil, err
	}
	if n != length {
		c.PutReadBuffer(buf)
		return nil, io.ErrUnexpectedEOF
	}
	return buf, nil
}

func (c *AioChannel) PutReadBuffer(buf []byte) {
	if len(buf) == 0 || cap(buf) > maxPoolBufSize {
		return
	}
	reusable := buf[:0]
	c.readPool.Put(&reusable)
}

func (c *AioChannel) Close() error {
	if c.stopped.Swap(true) {
		return nil // already closed
	}
	close(c.stopCh)
	c.loopWG.Wait()

	if c.aio == nil {
		return nil
	}
	err := c.aio.Close()
	c.aio = nil
	c.file = nil
	return err
}

func (c *AioChannel) getReadBuffer(length int) []byte {
	if length == 0 {
		return nil
	}
	if length > maxPoolBufSize {
		return make([]byte, length)
	}

	bufPtr := c.readPool.Get().(*[]byte)
	if cap(*bufPtr) < length {
		*bufPtr = make([]byte, length)
	}
	return (*bufPtr)[:length]
}

// AlignBlockSize rounds size up to the block-device IO alignment.
func AlignBlockSize(size int64) int64 {
	if size <= 0 {
		return 0
	}
	if size%BlockSize == 0 {
		return size
	}
	return (size/BlockSize + 1) * BlockSize
}

// NewAlignedBlockBuffer returns a zeroed buffer whose address and length are
// suitable for O_DIRECT/AIO writes against the block device.
func NewAlignedBlockBuffer(size int) []byte {
	if size <= 0 {
		return nil
	}
	raw := make([]byte, size+int(BlockSize)-1)
	addr := uintptr(unsafe.Pointer(&raw[0]))
	start := 0
	if rem := addr % uintptr(BlockSize); rem != 0 {
		start = int(uintptr(BlockSize) - rem)
	}
	return raw[start : start+size]
}

func isAlignedBuffer(data []byte) bool {
	if len(data) == 0 {
		return true
	}
	return uintptr(unsafe.Pointer(&data[0]))%uintptr(BlockSize) == 0 && int64(len(data))%BlockSize == 0
}

func validateAlignedResult(r Result) error {
	if r.Offset < 0 || r.Size <= 0 {
		return fmtAlignmentError("result", r.Offset, r.Size)
	}
	if r.Offset%BlockSize != 0 {
		return fmtAlignmentError("offset", r.Offset, r.Size)
	}
	if r.Size%BlockSize != 0 {
		return fmtAlignmentError("len", r.Offset, r.Size)
	}
	return nil
}

func fmtAlignmentError(kind string, offset, size int64) error {
	return fmt.Errorf("%s is not %d-byte aligned: offset=%d size=%d", kind, BlockSize, offset, size)
}
