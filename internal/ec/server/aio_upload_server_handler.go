package server

import (
	"context"

	"fmt"
	"hash/crc32"
	"sync"
	"sync/atomic"
	"time"

	mspebble "Corgi/internal/database/pebble"
	"Corgi/internal/define"
	"Corgi/internal/fs"
	"Corgi/internal/message/pb"

	"github.com/cockroachdb/pebble/v2"
	"github.com/gogo/protobuf/proto"
)

// writeMetrics 累计统计 AIO flush 的调用次数、字节数和耗时。
type writeMetrics struct {
	ops                atomic.Int64
	bytes              atomic.Int64
	nanos              atomic.Int64
	allocLatency       atomic.Int64
	batchCommitLatency atomic.Int64
}

var globalWriteMetrics writeMetrics

func RecordWriteStats(batchCommitLatency time.Duration) {
	recordWriteStats(0, 0, 0, batchCommitLatency)
}

func recordWriteStats(n int64, d time.Duration, allocLatency time.Duration, batchCommitLatency time.Duration) {
	globalWriteMetrics.ops.Add(1)
	globalWriteMetrics.bytes.Add(n)
	globalWriteMetrics.nanos.Add(d.Nanoseconds())
	globalWriteMetrics.allocLatency.Add(allocLatency.Nanoseconds())
	globalWriteMetrics.batchCommitLatency.Add(batchCommitLatency.Nanoseconds())
}

// WriteStatsSnapshot 是某一时刻的累计计数快照。
type WriteStatsSnapshot struct {
	Ops                int64
	Bytes              int64
	Nanos              int64
	AllocLatency       int64
	BatchCommitLatency int64
}

// SnapshotWriteStats 返回 AIO flush 当前累计计数器的快照。
func SnapshotWriteStats() WriteStatsSnapshot {
	return WriteStatsSnapshot{
		Ops:                globalWriteMetrics.ops.Load(),
		Bytes:              globalWriteMetrics.bytes.Load(),
		Nanos:              globalWriteMetrics.nanos.Load(),
		AllocLatency:       globalWriteMetrics.allocLatency.Load(),
		BatchCommitLatency: globalWriteMetrics.batchCommitLatency.Load(),
	}
}

type PayloadMetaType string

const (
	MetaContinue PayloadMetaType = "CONTINUE"
	MetaSuccess  PayloadMetaType = "SUCCESS"
	MetaError    PayloadMetaType = "ERROR"
)

type Response struct {
	Type PayloadMetaType
	Err  error
}

type AioUploadServerHandler struct {
	mu            sync.Mutex
	device        *fs.BlockDevice
	initReq       *pb.PutInitRequest
	chunks        [][]byte
	pendingLen    int
	fileSize      int64
	crc32Value    uint32
	blockInfoList []*flushBlockInfo
	flushWG       sync.WaitGroup
	flushErr      error
	finalized     bool
	responses     chan Response
	allocCount    int
}

type flushBlockInfo struct {
	offset      []int64
	length      []int64
	results     []fs.Result
	token       []fs.Result
	originalLen int
	alignedLen  int
	err         error
}

func NewAioUploadServerHandler(device *fs.BlockDevice, responses chan Response) *AioUploadServerHandler {
	return &AioUploadServerHandler{
		device:    device,
		responses: responses,
		chunks:    make([][]byte, 0, 16),
	}
}

func (h *AioUploadServerHandler) Start(req *pb.PutInitRequest) {
	h.initReq = req
	h.pendingLen = 0
	h.fileSize = 0
	h.crc32Value = 0
	h.blockInfoList = h.blockInfoList[:0]
	h.flushErr = nil
	h.finalized = false
}

func (h *AioUploadServerHandler) Put(data []byte) {
	var (
		info *flushBlockInfo
		buf  []byte
	)
	if h.initReq == nil {
		h.responses <- Response{Type: MetaError, Err: fmt.Errorf("start request is missing")}
		return
	}
	if h.flushErr != nil {
		err := h.flushErr
		h.responses <- Response{Type: MetaError, Err: err}
		return
	}
	if h.finalized {
		h.responses <- Response{Type: MetaError, Err: fmt.Errorf("upload is already finalized")}
		return
	}
	if len(data) == 0 {
		h.responses <- Response{Type: MetaContinue}
		return
	}

	// 只记录 CRC32，不再使用 sha256/hash.Hash
	h.crc32Value = crc32.Update(h.crc32Value, crc32.IEEETable, data)
	h.chunks = append(h.chunks, data)
	h.pendingLen += len(data)
	h.fileSize += int64(len(data))
	if h.pendingLen >= int(fs.MinAllocSize) {
		info, buf = h.startFlush()
	}

	if info != nil {
		go h.runFlush(context.Background(), info, buf, true)
		return
	}

	h.responses <- Response{Type: MetaContinue}
}

func (h *AioUploadServerHandler) Complete(ctx context.Context) {
	var (
		info *flushBlockInfo
		buf  []byte
	)

	initReq := h.initReq
	if initReq == nil {
		h.responses <- Response{Type: MetaError, Err: fmt.Errorf("start request is missing")}
		return
	}
	if h.flushErr != nil {
		flushErr := h.flushErr
		h.finalized = true
		h.flushWG.Wait()
		h.rollbackCompletedFlushes()
		h.responses <- Response{Type: MetaError, Err: flushErr}
		return
	}
	if h.finalized {
		h.responses <- Response{Type: MetaError, Err: fmt.Errorf("upload is already finalized")}
		return
	}
	if h.fileSize == 0 {
		h.finalized = true
		h.responses <- Response{Type: MetaError, Err: fmt.Errorf("empty put payload")}
		return
	}
	h.finalized = true
	if h.pendingLen != 0 {
		info, buf = h.startFlush()
	}

	if info != nil {
		go h.runFlush(ctx, info, buf, false)
	}
	h.flushWG.Wait()
	flushErr := h.flushErr
	fileSize := h.fileSize
	etag := ""
	if h.crc32Value != 0 {
		etag = fmt.Sprintf("%08x", h.crc32Value)
	}

	if flushErr != nil {
		h.rollbackCompletedFlushes()
		h.responses <- Response{Type: MetaError, Err: flushErr}
		return
	}

	fileMeta, results, totalSize := buildFileMetaAndResults(initReq, fileSize, etag, h.blockInfoList, h.allocCount)
	metaBytes, err := proto.Marshal(fileMeta)
	if err != nil {
		h.rollbackCompletedFlushes()
		h.responses <- Response{Type: MetaError, Err: err}
		return
	}

	lun := initReq.Lun
	if lun == "" {
		lun = h.device.Name()
	}

	exists := false
	consumer0 := func(_ *pebble.DB, batch *mspebble.WriteBatch, _ *mspebble.BatchRequest) error {
		// Java: writeBatch.merge(MSRocksDB.getColumnFamily(lun), data.var1, data.var2)
		if err := h.device.PersistAllocBatch(ctx, batch, results); err != nil {
			return err
		}
		if err := batch.Set([]byte(fileMeta.GetKey()), metaBytes); err != nil {
			return err
		}
		if err := batch.Merge([]byte(define.PebbleFileSystemFileNum), mspebble.ToByte(1)); err != nil {
			return err
		}
		if err := batch.Merge([]byte(define.PebbleFileSystemSize), mspebble.ToByte(fileMeta.Size)); err != nil {
			return err
		}
		if err := batch.Merge([]byte(define.PebbleFileSystemUsedSize), mspebble.ToByte(totalSize)); err != nil { //对齐后实际占用的空间大小
			return err
		}
		return nil
	}

	consumer := consumer0
	if !initReq.NoGet {
		consumer = func(db *pebble.DB, batch *mspebble.WriteBatch, req *mspebble.BatchRequest) error {
			oldValue, err := batch.GetFromBatchAndDB(db, fileMetaKey(fileMeta.MetaKey))
			if err != nil {
				return err
			}
			if oldValue == nil {
				return consumer0(db, batch, req)
			}
			exists = true
			return nil
		}
	}
	// consumer = func(db *pebble.DB, batch *mspebble.WriteBatch, req *mspebble.BatchRequest) error {
	// 	//do nothing
	// 	return nil
	// }
	if err = <-mspebble.CustomizeOperateData(lun, consumer); err != nil {
		h.rollbackCompletedFlushes()
		h.responses <- Response{Type: MetaError, Err: err}
		return
	}

	if exists {
		h.rollbackCompletedFlushes()
	}

	h.responses <- Response{Type: MetaSuccess}
}

func (h *AioUploadServerHandler) TimeOut() {
	h.mu.Lock()
	h.finalized = true
	h.pendingLen = 0
	for i := range h.chunks {
		h.chunks[i] = nil
	}
	h.chunks = h.chunks[:0]
	h.mu.Unlock()

	h.flushWG.Wait()
	h.rollbackCompletedFlushes()
	h.responses <- Response{Type: MetaError, Err: fmt.Errorf("channel timeout")}
}

func (h *AioUploadServerHandler) startFlush() (*flushBlockInfo, []byte) {
	originalLen := h.pendingLen
	alignedLen := int(fs.AlignBlockSize(int64(originalLen)))
	buf := fs.NewAlignedBlockBuffer(alignedLen)

	cursor := 0
	for i, chunk := range h.chunks {
		copy(buf[cursor:], chunk)
		cursor += len(chunk)
		h.chunks[i] = nil
	}

	h.chunks = h.chunks[:0]
	h.pendingLen = 0

	info := &flushBlockInfo{
		originalLen: originalLen,
		alignedLen:  alignedLen,
	}
	h.blockInfoList = append(h.blockInfoList, info)
	h.flushWG.Add(1)
	return info, buf
}

func (h *AioUploadServerHandler) runFlush(ctx context.Context, info *flushBlockInfo, buf []byte, respond bool) {
	done := false
	defer func() {
		if !done {
			h.flushWG.Done()
		}
	}()

	allocStart := time.Now()
	results, token, err := h.device.Alloc(int64(len(buf)))
	allocElapsed := time.Since(allocStart)
	if err != nil {
		h.failFlush(info, err)
		if respond {
			h.flushWG.Done()
			done = true
			h.flushWG.Wait()
			h.rollbackCompletedFlushes()
			h.responses <- Response{Type: MetaError, Err: err}
		}
		return
	}

	writeStart := time.Now()
	written, err := h.device.Channel.WriteAllocated(ctx, buf, results)
	writeElapsed := time.Since(writeStart)
	if err == nil && written != len(buf) {
		err = fmt.Errorf("partial aio write: need=%d written=%d", len(buf), written)
	}
	if err != nil {
		h.device.RollbackAlloc(token)
		h.failFlush(info, err)
		if respond {
			h.flushWG.Done()
			done = true
			h.flushWG.Wait()
			h.rollbackCompletedFlushes()
			h.responses <- Response{Type: MetaError, Err: err}
		}
		return
	}

	h.completeFlush(info, results, token)
	recordWriteStats(int64(len(buf)), writeElapsed, allocElapsed, 0)
	h.flushWG.Done()
	done = true
	if respond {
		h.responses <- Response{Type: MetaContinue}
	}
}

func (h *AioUploadServerHandler) completeFlush(info *flushBlockInfo, results []fs.Result, token []fs.Result) {
	info.results = results
	info.token = token
	h.allocCount += len(results)
}

func (h *AioUploadServerHandler) failFlush(info *flushBlockInfo, err error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	info.err = err
	if h.flushErr == nil {
		h.flushErr = err
	}
}

func (h *AioUploadServerHandler) rollbackCompletedFlushes() {
	h.mu.Lock()
	tokens := make([][]fs.Result, 0, len(h.blockInfoList))
	for _, info := range h.blockInfoList {
		if len(info.token) == 0 {
			continue
		}
		tokens = append(tokens, append([]fs.Result(nil), info.token...))
		info.token = nil
	}
	h.mu.Unlock()

	for _, token := range tokens {
		h.device.RollbackAlloc(token)
	}
}

func flattenBlockResults(blockInfoList []*flushBlockInfo) []fs.Result {
	results := make([]fs.Result, 0, len(blockInfoList))
	for _, info := range blockInfoList {
		results = append(results, info.results...)
	}
	return results
}

func buildFileMeta(req *pb.PutInitRequest, total int64, etag string, blockInfoList []*flushBlockInfo) *pb.FileMeta {
	meta := &pb.FileMeta{
		MetaKey:  req.MetaKey,
		FileName: req.FileName,
		Lun:      req.Lun,
		Etag:     etag,
		Size:     total,
		Offset:   make([]int64, 0, len(blockInfoList)),
		Len:      make([]int64, 0, len(blockInfoList)),
	}
	for _, info := range blockInfoList {
		meta.Offset = append(meta.Offset, info.offset...)
		meta.Len = append(meta.Len, info.length...)
	}
	return meta
}

func fileMetaKey(metaKey string) []byte {
	return []byte("meta/file/" + metaKey)
}

func marshalFileMeta(meta *pb.FileMeta) ([]byte, error) {
	// TODO: Replace with protobuf marshal generated from api/proto/meta.
	// For now we use a stable binary layout to keep implementation runnable.
	return encodeFileMetaBinary(meta)
}

func buildFileMetaAndResults(req *pb.PutInitRequest, total int64, etag string, blockInfoList []*flushBlockInfo, allocCount int) (*pb.FileMeta, []fs.Result, int64) {
	meta := &pb.FileMeta{
		MetaKey:  req.MetaKey,
		FileName: req.FileName,
		Lun:      req.Lun,
		Etag:     etag,
		Size:     total,
		Offset:   make([]int64, 0, allocCount),
		Len:      make([]int64, 0, allocCount),
	}

	results := make([]fs.Result, 0, allocCount)
	totalSize := int64(0)
	for _, info := range blockInfoList {
		for _, r := range info.results {
			meta.Offset = append(meta.Offset, r.Offset)
			meta.Len = append(meta.Len, r.Size)
			results = append(results, r)
			totalSize += r.Size
		}
	}

	return meta, results, totalSize
}
