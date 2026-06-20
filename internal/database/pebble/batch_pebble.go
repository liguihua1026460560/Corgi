package mspebble // import "Corgi/internal/database/pebble"

import (
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble/v2"
)

// ─── Types ───────────────────────────────────────────────────────────────────

// RequestType identifies the kind of write operation inside a BatchRequest.
type RequestType int

const (
	// CustomizeOperate executes a caller-provided RequestConsumer.
	// This is the only fully implemented type; all others are reserved.
	CustomizeOperate RequestType = iota

	// PutOnly writes each Keys[i] → Values[i] directly into the batch.
	// Equivalent to Java BatchRocksDB.PUT_ONLY. TODO: implement when needed.
	PutOnly

	// CompareAndPut writes the new value only when the existing versionNum
	// (JSON field) is strictly less than the incoming one.
	// Equivalent to Java BatchRocksDB.COMPARE_AND_PUT. TODO: implement when needed.
	CompareAndPut

	// DeleteAndPut deletes all Keys and writes one new key/value pair.
	// Equivalent to Java BatchRocksDB.DELETE_AND_PUT. TODO: implement when needed.
	DeleteAndPut
)

// RequestConsumer is the Go equivalent of Java BatchRocksDB.RequestConsumer.
//
// Both the raw *pebble.DB and the currently-open *WriteBatch (IndexedBatch)
// are provided. The consumer must NOT call batch.Commit() or batch.Close();
// those operations are owned by the BatchPebble processor.
//
// Read-your-writes guarantee:
//
//	Because the batch is created via db.NewIndexedBatch() (Pebble's equivalent
//	of RocksDB WriteBatchWithIndex(readYourOwn=true)), any key written with
//	batch.Set()/batch.Put() is immediately visible to subsequent batch.Get()
//	calls within the same batch, enabling within-batch read-your-writes
//	semantics.
type RequestConsumer func(db *pebble.DB, batch *WriteBatch, req *BatchRequest) error

// BatchRequest is a single unit of work submitted to a BatchPebble processor.
// Equivalent to Java BatchRocksDB.BatchRequest.
type BatchRequest struct {
	Type     RequestType
	Consumer RequestConsumer // used only for CustomizeOperate

	// Keys / Values carry operand data for PutOnly, CompareAndPut,
	// DeleteAndPut, and Merge. Unused for CustomizeOperate.
	Keys   [][]byte
	Values [][]byte

	// LowPriority requests are executed after all normal requests in the same
	// flush batch, mirroring Java BatchRocksDB's two-pass ordering.
	LowPriority bool

	// result delivers the commit outcome to the caller (nil = success).
	// Buffered with capacity 1 so the processor never blocks.
	result chan error

	// done prevents double-notification when the processor stops mid-batch.
	done atomic.Bool
}

// Done returns a receive-only channel that yields nil on success or an error
// on failure once the encompassing batch is committed (or abandoned).
func (r *BatchRequest) Done() <-chan error {
	return r.result
}

// ─── BatchPebble ─────────────────────────────────────────────────────────────

// processLen is the number of processor channels created for a meta-pool
// BatchPebble. Data-pool BatchPebble always uses exactly 1 processor.
const processLen = 4

// maxBatchSize is the maximum number of requests accumulated before flushing.
// Matches Java BatchRocksDB list.size() > 1000 guard.
const maxBatchSize = 1000

// flushInterval is the maximum idle wait before a non-empty pending batch is
// committed. Avoids indefinitely deferred writes under low throughput.
const flushInterval = 2 * time.Millisecond

// BatchPebble is the Go/Pebble equivalent of Java BatchRocksDB.
//
// Each LUN has exactly one registered BatchPebble instance (held in batchMap).
// Callers submit work via CustomizeOperateData() / CustomizeOperateMeta().
//
// Architecture:
//
//	Data pool (active):  1 processor goroutine draining a single channel.
//	Meta pool (reserved): processLen=4 goroutines, each draining its own
//	                      channel. Key routing: abs(hashCode) % processLen.
//	                      Activate by using newMetaBatchPebble() and routing
//	                      submissions through dispatchMeta().
//
// Flush semantics:
//
//  1. Accumulate up to maxBatchSize requests (or wait flushInterval for more).
//  2. Create a fresh NewWriteBatch().
//  3. Execute normal requests first, then LowPriority requests.
//  4. Commit with pebble.Sync (≡ Java WRITE_OPTIONS.setSync(true)).
//  5. Notify every request with the commit result.
//  6. Always call batch.Close() regardless of outcome.
type BatchPebble struct {
	db         *pebble.DB
	lun        string
	isMetaPool bool // reserved; not active for data pool

	// dataProc is the single data-pool processor channel.
	dataProc chan *BatchRequest

	// metaProcs holds processLen channels for meta-pool routing.
	// Only populated when isMetaPool==true (reserved, not yet used).
	//
	// To activate meta pool:
	//   1. Allocate bp.metaProcs = make([]chan *BatchRequest, processLen)
	//   2. For each i, make(chan *BatchRequest, 4096) + go bp.runProcessor(…)
	//   3. In dispatchMeta(hashCode, req): bp.metaProcs[abs(hashCode)%processLen] <- req
	metaProcs []chan *BatchRequest

	stopOnce sync.Once
	stopCh   chan struct{}
	wg       sync.WaitGroup
}

// ─── Global registry ─────────────────────────────────────────────────────────

var batchMap sync.Map // lun (string) → *BatchPebble

// AddDataBatch registers a data-pool BatchPebble for lun.
// Equivalent to Java BatchRocksDB.addBatch() for non-SSD / data pool.
func AddDataBatch(lun string, db *pebble.DB) {
	bp := newBatchPebble(false, lun, db)
	batchMap.Store(lun, bp)
}

// NOTE: AddMetaBatch is reserved for the meta (index) pool.
// When activated it should create processLen=4 processor goroutines and route
// submissions via abs(hashCode) % processLen.
// func AddMetaBatch(lun string, db *pebble.DB) { ... }

// RemoveBatch removes and gracefully stops the BatchPebble for lun.
// Equivalent to Java BatchRocksDB.remove(lun).
func RemoveBatch(lun string) {
	v, ok := batchMap.LoadAndDelete(lun)
	if !ok {
		return
	}
	v.(*BatchPebble).stop()
}

// ─── Constructor ─────────────────────────────────────────────────────────────

func newBatchPebble(isMetaPool bool, lun string, db *pebble.DB) *BatchPebble {
	bp := &BatchPebble{
		db:         db,
		lun:        lun,
		isMetaPool: isMetaPool,
		dataProc:   make(chan *BatchRequest, 10000),
		stopCh:     make(chan struct{}),
	}

	if isMetaPool {
		// ── Meta pool: 4 processors (reserved) ──────────────────────────────
		//
		// Activate this block when meta pool is needed:
		//
		//   bp.metaProcs = make([]chan *BatchRequest, processLen)
		//   for i := 0; i < processLen; i++ {
		//       bp.metaProcs[i] = make(chan *BatchRequest, 4096)
		//       bp.wg.Add(1)
		//       go bp.runProcessor(bp.metaProcs[i])
		//   }
		//   return bp  // skip the data processor below
		//
		// Routing in dispatchMeta(hashCode int, req *BatchRequest):
		//   index := hashCode
		//   if index < 0 { index = -index }
		//   bp.metaProcs[index%processLen] <- req
		//
		log.Printf("[BatchPebble] meta pool for lun %q is reserved; falling back to single data processor", lun)
	}

	// Data pool: single processor goroutine.
	bp.wg.Add(1)
	go bp.runProcessor(bp.dataProc)
	return bp
}

// stop signals all processors to drain and waits for them to exit. Idempotent.
func (bp *BatchPebble) stop() {
	bp.stopOnce.Do(func() { close(bp.stopCh) })
	bp.wg.Wait()
}

// dispatch routes a request to the data processor.
// If the processor is already stopped the request is immediately failed.
func (bp *BatchPebble) dispatch(req *BatchRequest) {
	select {
	case bp.dataProc <- req:
	case <-bp.stopCh:
		if req.done.CompareAndSwap(false, true) {
			req.result <- fmt.Errorf("BatchPebble for lun %q is stopped", bp.lun)
		}
	}
}

// ─── Processor loop ──────────────────────────────────────────────────────────

// runProcessor is the long-lived goroutine that accumulates BatchRequests and
// flushes them atomically.
//
//  1. Block until the first request arrives (or shutdown).
//  2. Drain additional requests up to maxBatchSize.
//  3. If the channel drains to empty, wait up to flushInterval for more.
//  4. Call flush() with the accumulated slice.
//  5. Repeat.
func (bp *BatchPebble) runProcessor(ch chan *BatchRequest) {
	defer bp.wg.Done()
	batch := NewWriteBatch(bp.db, bp.lun)
	defer batch.Close()

	requests := make([]*BatchRequest, 0, maxBatchSize)

	for {
		var first *BatchRequest

		// 1. 阻塞等待第一个请求，或者收到停止信号
		select {
		case <-bp.stopCh:
			bp.drainOnStop(ch)
			return
		case first = <-ch:
		}

		requests := requests[:0] // reset slice while keeping capacity
		requests = append(requests, first)

		// 2. 非阻塞 drain channel，当前有多少取多少，最多 1000 个
	drainLoop:
		for len(requests) < maxBatchSize {
			select {
			case req := <-ch:
				requests = append(requests, req)
			default:
				// channel 当前为空，直接下刷
				break drainLoop
			}
		}
		// 3. 统一提交
		bp.flush(batch, requests)
		// 4. 如果已经停止，清理队列里剩余请求后退出
		select {
		case <-bp.stopCh:
			bp.drainOnStop(ch)
			return
		default:
		}
	}
}

// drainOnStop fails all requests still queued in ch with a "stopped" error.
// Called when the processor is shutting down.
func (bp *BatchPebble) drainOnStop(ch chan *BatchRequest) {
	err := fmt.Errorf("BatchPebble for lun %q stopped", bp.lun)
	for {
		select {
		case req := <-ch:
			if req.done.CompareAndSwap(false, true) {
				req.result <- err
			}
		default:
			return
		}
	}
}

// ─── Flush ───────────────────────────────────────────────────────────────────

// flush executes a slice of BatchRequests atomically inside a single Pebble
// WriteBatch, then commits it with pebble.Sync.
//
// Design decisions:
//
//  1. NewWriteBatch() ─ wraps Pebble's IndexedBatch, equivalent to RocksDB's
//     WriteBatchWithIndex(readYourOwn=true). Any Set() inside a RequestConsumer
//     is immediately visible to subsequent Get() calls on the same batch,
//     providing within-batch read-your-writes semantics.
//
//  2. Normal requests run first; LowPriority requests run second, matching
//     Java BatchRocksDB's two-pass loop ordering.
//
//  3. If any handler returns an error the entire batch is abandoned (not
//     committed). Every request in the batch receives the error. No partial
//     commit is ever made.
//
//  4. batch.Commit(pebble.Sync) ─ equivalent to Java WRITE_OPTIONS.setSync(true).
//     Data is fsync'd to stable storage before returning.
//
//  5. batch.Close() is always called (via defer), even if Commit fails.
func (bp *BatchPebble) flush(batch *WriteBatch, requests []*BatchRequest) {
	// WriteBatch wraps an IndexedBatch: Get() reflects in-batch writes.

	defer batch.Clear() // Reset the batch for reuse in the next flush, avoiding repeated allocations.

	// Split into normal and low-priority without mutating the input slice.
	normal := make([]*BatchRequest, 0, len(requests))
	low := make([]*BatchRequest, 0)
	for _, req := range requests {
		if req.LowPriority {
			low = append(low, req)
		} else {
			normal = append(normal, req)
		}
	}

	// Execute normal requests first, then low-priority requests.
	// If any request fails, the entire batch is aborted.
	var batchErr error
	for _, req := range normal {
		if err := bp.handleRequest(batch, req); err != nil {
			batchErr = err
			break
		}
	}
	if batchErr == nil {
		for _, req := range low {
			if err := bp.handleRequest(batch, req); err != nil {
				batchErr = err
				break
			}
		}
	}

	// Commit or propagate error.
	if batchErr == nil {
		// pebble.Sync ↔ Java WRITE_OPTIONS.setSync(true): fsync before return.
		batchErr = batch.Commit(pebble.Sync)
	}

	// Notify every request of the outcome.
	for _, req := range requests {
		if req.done.CompareAndSwap(false, true) {
			req.result <- batchErr
		}
	}
}

// handleRequest dispatches a single request to its typed handler.
func (bp *BatchPebble) handleRequest(batch *WriteBatch, req *BatchRequest) error {
	switch req.Type {
	case CustomizeOperate:
		return handleCustomizeOperate(bp.db, batch, req)
	case PutOnly:
		return handlePutOnly(batch, req)
	case CompareAndPut:
		return handleCompareAndPut(batch, req)
	case DeleteAndPut:
		return handleDeleteAndPut(batch, req)
	default:
		return fmt.Errorf("BatchPebble: unknown request type %d", req.Type)
	}
}

// ─── Handler implementations ─────────────────────────────────────────────────

// handleCustomizeOperate executes the caller-provided consumer.
//
// This is the only fully implemented handler. The consumer receives:
//   - db:    the raw *pebble.DB (for operations outside the batch)
//   - batch: the open IndexedBatch (for in-batch read-write operations)
//
// Because batch is an IndexedBatch, any batch.Set() made inside the consumer
// is immediately visible to subsequent batch.Get() calls within the same
// batch – matching Java WriteBatchWithIndex(readYourOwn=true).
func handleCustomizeOperate(db *pebble.DB, batch *WriteBatch, req *BatchRequest) error {
	if req.Consumer == nil {
		return errors.New("CustomizeOperate: Consumer must not be nil")
	}
	return req.Consumer(db, batch, req)
}

// handlePutOnly writes each Keys[i] → Values[i] directly into the batch.
//
// Equivalent to Java BatchRocksDB.PUT_ONLY.
func handlePutOnly(batch *WriteBatch, req *BatchRequest) error {
	if len(req.Keys) != len(req.Values) {
		return fmt.Errorf("PUT_ONLY: keys/values length mismatch: %d/%d", len(req.Keys), len(req.Values))
	}
	for i, key := range req.Keys {
		if err := batch.Put(key, req.Values[i]); err != nil {
			return err
		}
	}
	return nil
}

// handleCompareAndPut reads the existing JSON versionNum for each key and
// writes the new value only when it is strictly greater (newer).
//
// Equivalent to Java BatchRocksDB.COMPARE_AND_PUT.
// TODO: implement JSON versionNum comparison when this code path is needed.
func handleCompareAndPut(_ *WriteBatch, _ *BatchRequest) error {
	// Implementation sketch:
	//   for i, key := range req.Keys {
	//       existing, closer, err := batch.Get(key)  // IndexedBatch: reflects in-batch writes
	//       if closer != nil { defer closer.Close() }
	//       if err != nil && !errors.Is(err, pebble.ErrNotFound) { return err }
	//       if versionNumGreater(req.Values[i], existing) {
	//           if err := batch.Set(key, req.Values[i], nil); err != nil { return err }
	//       }
	//   }
	return errors.New("COMPARE_AND_PUT not implemented")
}

// handleDeleteAndPut deletes all Keys and writes one new key/value pair.
//
// Equivalent to Java BatchRocksDB.DELETE_AND_PUT.
func handleDeleteAndPut(batch *WriteBatch, req *BatchRequest) error {
	for _, key := range req.Keys {
		if err := batch.Delete(key); err != nil {
			return err
		}
	}
	if len(req.Values) == 0 {
		return nil
	}
	if len(req.Keys) == 0 {
		return errors.New("DELETE_AND_PUT: missing put key")
	}
	return batch.Put(req.Keys[len(req.Keys)-1], req.Values[0])
}

// ─── Public API ──────────────────────────────────────────────────────────────

// CustomizeOperateData submits a CustomizeOperate request to the data-pool
// processor for lun. Returns a channel that yields nil on success or an error
// on failure once the encompassing batch is committed (or abandoned).
//
// Equivalent to Java BatchRocksDB.customizeOperateData(lun, consumer).
func CustomizeOperateData(lun string, consumer RequestConsumer) <-chan error {
	return submitRequest(lun, consumer, false)
}

// CustomizeOperateDataLowPriority is the low-priority variant.
// Low-priority requests are executed after all normal requests in the same
// flush batch.
//
// Equivalent to Java BatchRocksDB.customizeOperateDataForLowPriority().
func CustomizeOperateDataLowPriority(lun string, consumer RequestConsumer) <-chan error {
	return submitRequest(lun, consumer, true)
}

// CustomizeOperateMeta is the meta-pool equivalent of CustomizeOperateData.
// hashCode is used to route to one of processLen processors (key-affinity).
//
// NOTE: Meta pool is currently NOT active. This function falls back to the
// single data-pool processor until meta pool is explicitly enabled.
//
// When meta pool is activated:
//
//	index := hashCode
//	if index < 0 { index = -index }
//	bp.metaProcs[index % processLen] <- req
func CustomizeOperateMeta(lun string, _ int, consumer RequestConsumer) <-chan error {
	// TODO: route to bp.metaProcs[abs(hashCode)%processLen] when meta pool is active.
	return submitRequest(lun, consumer, false)
}

// CustomizeOperateMetaLowPriority is the low-priority meta-pool variant.
// Falls back to the data pool until meta pool is activated.
func CustomizeOperateMetaLowPriority(lun string, _ int, consumer RequestConsumer) <-chan error {
	// TODO: route to metaProcs when meta pool is active.
	return submitRequest(lun, consumer, true)
}

// submitRequest constructs a BatchRequest and dispatches it to the registered
// BatchPebble for lun. If no BatchPebble exists for lun, the request is
// immediately failed.
func submitRequest(lun string, consumer RequestConsumer, lowPriority bool) <-chan error {
	req := &BatchRequest{
		Type:        CustomizeOperate,
		Consumer:    consumer,
		LowPriority: lowPriority,
		result:      make(chan error, 1),
	}
	v, ok := batchMap.Load(lun)
	if !ok {
		req.done.Store(true)
		req.result <- fmt.Errorf("BatchPebble: no processor registered for lun %q", lun)
		return req.result
	}
	v.(*BatchPebble).dispatch(req)
	return req.result
}

func ToByte(c int64) []byte {
	res := make([]byte, 8)
	binary.LittleEndian.PutUint64(res, uint64(c))
	return res
}
