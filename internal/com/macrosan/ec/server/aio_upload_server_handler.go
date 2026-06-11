package server

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble/v2"
	mspebble "mossserver/internal/com/macrosan/database/pebble"
	"mossserver/internal/com/macrosan/fs"
	"mossserver/internal/com/macrosan/message/pb"
)

// writeMetrics 累计统计 AIO flush 的调用次数、字节数和耗时。
type writeMetrics struct {
	ops          atomic.Int64
	bytes        atomic.Int64
	nanos        atomic.Int64
	allocLatency atomic.Int64
}

var globalWriteMetrics writeMetrics

func recordWriteStats(n int64, d time.Duration, allocLatency time.Duration) {
	globalWriteMetrics.ops.Add(1)
	globalWriteMetrics.bytes.Add(n)
	globalWriteMetrics.nanos.Add(d.Nanoseconds())
	globalWriteMetrics.allocLatency.Add(allocLatency.Nanoseconds())
}

// WriteStatsSnapshot 是某一时刻的累计计数快照。
type WriteStatsSnapshot struct {
	Ops          int64
	Bytes        int64
	Nanos        int64
	AllocLatency int64
}

// SnapshotWriteStats 返回 AIO flush 当前累计计数器的快照。
func SnapshotWriteStats() WriteStatsSnapshot {
	return WriteStatsSnapshot{
		Ops:          globalWriteMetrics.ops.Load(),
		Bytes:        globalWriteMetrics.bytes.Load(),
		Nanos:        globalWriteMetrics.nanos.Load(),
		AllocLatency: globalWriteMetrics.allocLatency.Load(),
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
	digest        hash.Hash
	blockInfoList []*flushBlockInfo
	flushWG       sync.WaitGroup
	flushErr      error
	finalized     bool
	responses     chan Response
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
	h.digest = sha256.New()
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
	if h.digest != nil {
		etag = hex.EncodeToString(h.digest.Sum(nil))
	}
	blockInfoList := append([]*flushBlockInfo(nil), h.blockInfoList...)

	if flushErr != nil {
		h.rollbackCompletedFlushes()
		h.responses <- Response{Type: MetaError, Err: flushErr}
		return
	}

	meta := buildFileMeta(initReq, fileSize, etag, blockInfoList)
	metaBytes, err := marshalFileMeta(meta)
	if err != nil {
		h.rollbackCompletedFlushes()
		h.responses <- Response{Type: MetaError, Err: err}
		return
	}
	results := flattenBlockResults(blockInfoList)

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

		// Java: writeBatch.put(data.var1, data.var2)
		if err := batch.Set(fileMetaKey(meta.MetaKey), metaBytes); err != nil {
			return err
		}

		// TODO: Java 还会更新这些统计计数，Go 版暂不实现：
		// writeBatch.merge(ROCKS_FILE_SYSTEM_FILE_NUM, BatchRocksDB.toByte(1L));
		// writeBatch.merge(ROCKS_FILE_SYSTEM_FILE_SIZE, BatchRocksDB.toByte(fileMeta.getSize()));
		// writeBatch.merge(ROCKS_FILE_SYSTEM_USED_SIZE, BatchRocksDB.toByte(finalTotalLen));
		return nil
	}

	consumer := consumer0
	if !initReq.NoGet {
		consumer = func(db *pebble.DB, batch *mspebble.WriteBatch, req *mspebble.BatchRequest) error {
			oldValue, err := batch.GetFromBatchAndDB(db, fileMetaKey(meta.MetaKey))
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

	if h.digest == nil {
		h.digest = sha256.New()
	}

	cursor := 0
	for i, chunk := range h.chunks {
		_, _ = h.digest.Write(chunk)
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
	recordWriteStats(int64(len(buf)), writeElapsed, allocElapsed)
	h.flushWG.Done()
	done = true
	if respond {
		h.responses <- Response{Type: MetaContinue}
	}
}

func (h *AioUploadServerHandler) completeFlush(info *flushBlockInfo, results []fs.Result, token []fs.Result) {
	offset := make([]int64, 0, len(results))
	length := make([]int64, 0, len(results))
	for _, r := range results {
		offset = append(offset, r.Offset)
		length = append(length, r.Size)
	}

	// h.mu.Lock()
	// defer h.mu.Unlock()
	info.offset = offset
	info.length = length
	info.results = append([]fs.Result(nil), results...)
	info.token = append([]fs.Result(nil), token...)
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
