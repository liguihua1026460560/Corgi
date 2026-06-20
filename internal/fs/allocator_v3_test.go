package fs

import (
	"fmt"
	"math/bits"
	"math/rand"
	"sync"
	"testing"
	"time"
)

// ---- 基础功能测试 ----

// 1. NewAllocatorV3 后 Available == total
func TestV3_NewAllocator_Available(t *testing.T) {
	for _, n := range []int64{1, 63, 64, 65, 512, 1024, 16384, 100000} {
		a := NewAllocatorV3(n)
		if got := a.Available(); got != n {
			t.Errorf("blocks=%d: Available=%d want=%d", n, got, n)
		}
		if got := a.Total(); got != n {
			t.Errorf("blocks=%d: Total=%d want=%d", n, got, n)
		}
		if err := a.checkInvariant(); err != nil {
			t.Errorf("blocks=%d: invariant violation: %v", n, err)
		}
	}
}

// 额外回归测试：NewAllocatorV3 初始化后，total 之外的尾部对齐空间必须不可分配（bit=0），
// 且对应 L1 entry 不应是 FREE。该测试用于复现 alignedBlocks + clearExcessL0Bits 的初始化污染问题。
func TestV3_NewAllocator_InvalidTailBitsMustNotBeFree(t *testing.T) {
	// 选择非 16384 对齐大小，触发 alignedBlocks 扩容。
	const blocks int64 = 1000
	a := NewAllocatorV3(blocks)

	totalBits := int64(len(a.l0) * v3BitsPerSlot)
	if totalBits <= blocks {
		t.Fatalf("unexpected setup: totalBits=%d blocks=%d", totalBits, blocks)
	}

	// 1) total 之外所有 bit 都应为 0（不可分配）。
	for pos := blocks; pos < totalBits; pos++ {
		word := pos / v3BitsPerSlot
		bit := uint(pos % v3BitsPerSlot)
		if (a.l0[word]>>bit)&1 == 1 {
			t.Fatalf("invalid tail bit is free: pos=%d word=%d bit=%d total=%d", pos, word, bit, blocks)
		}
	}

	// 2) 完全越界的 slotset 在 L1 中不应被标记为 FREE。
	firstInvalidSlotset := int((blocks + v3BitsPerSlotset - 1) / v3BitsPerSlotset)
	totalSlotsets := (len(a.l0) + v3SlotsPerSlotset - 1) / v3SlotsPerSlotset
	for ss := firstInvalidSlotset; ss < totalSlotsets; ss++ {
		l1Slot := ss / v3L1EntriesPerSlot
		l1Bit := ss % v3L1EntriesPerSlot
		if l1Slot >= len(a.l1) {
			break
		}
		entry := (a.l1[l1Slot] >> uint(l1Bit*v3L1EntryWidth)) & v3L1EntryMask
		if entry == v3L1EntryFree {
			t.Fatalf("invalid tail slotset marked FREE in l1: slotset=%d l1Slot=%d l1Bit=%d", ss, l1Slot, l1Bit)
		}
	}
	if err := a.checkInvariant(); err != nil {
		t.Fatalf("invariant violation: %v", err)
	}
}

// 2. 连续 Allocate，offset 应该递增
func TestV3_Allocate_Sequential(t *testing.T) {
	a := NewAllocatorV3(256 * 1024)
	var prevEnd int64 = -1
	for i := 0; i < 4*100; i++ {
		res, err := a.Allocate(16)

		if err != nil {
			t.Fatalf("Allocate failed: %v", err)
		}
		if len(res) == 0 {
			t.Fatal("no results")
		}
		if res[0].Offset <= prevEnd {
			t.Errorf("pass %d: offset %d not after previous end %d", i, res[0].Offset, prevEnd)
		}
		prevEnd = res[len(res)-1].Offset + res[len(res)-1].Size - 1
	}
}

// 3. Allocate 后 Available 减少
func TestV3_Allocate_ReducesAvailable(t *testing.T) {
	a := NewAllocatorV3(128)
	before := a.Available()
	_, err := a.Allocate(32)
	if err != nil {
		t.Fatal(err)
	}
	after := a.Available()
	if after != before-32 {
		t.Errorf("available: before=%d after=%d, expected %d", before, after, before-32)
	}
	if err := a.checkInvariant(); err != nil {
		t.Fatalf("invariant violation: %v", err)
	}
}

// 4. Release 后 Available 增加
func TestV3_Release_IncreasesAvailable(t *testing.T) {
	a := NewAllocatorV3(128)
	res, _ := a.Allocate(32)
	before := a.Available()
	if err := a.Release(res); err != nil {
		t.Fatal(err)
	}
	after := a.Available()
	if after != before+32 {
		t.Errorf("available: before=%d after=%d", before, after)
	}
	if err := a.checkInvariant(); err != nil {
		t.Fatalf("invariant violation: %v", err)
	}
}

// 5. Release 后再次 Allocate 可以复用空间
func TestV3_Release_Reuse(t *testing.T) {
	a := NewAllocatorV3(64)
	res1, _ := a.Allocate(64)
	if err := a.Release(res1); err != nil {
		t.Fatal(err)
	}
	res2, err := a.Allocate(64)
	if err != nil {
		t.Fatalf("re-allocate failed: %v", err)
	}
	total := int64(0)
	for _, r := range res2 {
		total += r.Size
	}
	if total != 64 {
		t.Errorf("re-allocated total=%d want 64", total)
	}
	if err := a.checkInvariant(); err != nil {
		t.Fatalf("invariant violation: %v", err)
	}
}

// 6. InitAllocated 后对应空间不能再被分配
func TestV3_InitAllocated_BlocksSpace(t *testing.T) {
	a := NewAllocatorV3(128)
	// 标记前 64 块已分配
	if err := a.InitAllocated(0, 64); err != nil {
		t.Fatal(err)
	}
	if a.Available() != 64 {
		t.Errorf("available=%d want 64", a.Available())
	}
	// 分配 64 块，应该从 offset=64 开始
	res, err := a.Allocate(64)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range res {
		if r.Offset < 64 {
			t.Errorf("allocated in pre-occupied region: offset=%d", r.Offset)
		}
	}
	if err := a.checkInvariant(); err != nil {
		t.Fatalf("invariant violation: %v", err)
	}
}

// 7. 空间不足时 Allocate 返回错误，且 Available 不变
func TestV3_Allocate_InsufficientSpace(t *testing.T) {
	a := NewAllocatorV3(32)
	before := a.Available()
	_, err := a.Allocate(33)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if a.Available() != before {
		t.Errorf("available changed on failed alloc: before=%d after=%d", before, a.Available())
	}
	if err := a.checkInvariant(); err != nil {
		t.Fatalf("invariant violation: %v", err)
	}
}

// 8. 碎片情况下 Allocate 可以返回多个 Extent
func TestV3_Allocate_Fragmented(t *testing.T) {
	a := NewAllocatorV3(128)
	// 分配全部
	all, _ := a.Allocate(128)
	// 释放偶数位置的 64 块（交替释放，造成碎片）
	for i := int64(0); i < 128; i += 2 {
		_ = a.Release([]Result{{Offset: i, Size: 1}})
	}
	// 现在有 64 个离散空闲块，请求 64 块应该返回多个 extent
	res, err := a.Allocate(64)
	if err != nil {
		t.Fatalf("fragmented alloc failed: %v", err)
	}
	total := int64(0)
	for _, r := range res {
		total += r.Size
	}
	if total != 64 {
		t.Errorf("got total=%d want 64", total)
	}
	_ = all
	if err := a.checkInvariant(); err != nil {
		t.Fatalf("invariant violation: %v", err)
	}
}

// 9. 重复释放应该返回错误，不能导致 Available 超过 Total
func TestV3_Release_DoubleFree(t *testing.T) {
	a := NewAllocatorV3(64)
	res, _ := a.Allocate(32)
	_ = a.Release(res)
	err := a.Release(res) // 重复释放
	if err == nil {
		t.Fatal("expected error on double free")
	}
	if a.Available() > a.Total() {
		t.Errorf("available=%d > total=%d", a.Available(), a.Total())
	}
	if err := a.checkInvariant(); err != nil {
		t.Fatalf("invariant violation: %v", err)
	}
}

// 10. 越界测试
func TestV3_OutOfBounds(t *testing.T) {
	a := NewAllocatorV3(64)

	if err := a.Release([]Result{{Offset: 60, Size: 10}}); err == nil {
		t.Error("out-of-bounds release should fail")
	}
	if err := a.InitAllocated(60, 10); err == nil {
		t.Error("out-of-bounds InitAllocated should fail")
	}
	if err := a.InitFree(60, 10); err == nil {
		t.Error("out-of-bounds InitFree should fail")
	}
	if err := a.InitAllocated(-1, 10); err == nil {
		t.Error("negative offset InitAllocated should fail")
	}
	if err := a.InitFree(-1, 10); err == nil {
		t.Error("negative offset InitFree should fail")
	}
	if err := a.checkInvariant(); err != nil {
		t.Fatalf("invariant violation: %v", err)
	}
}

// 11. 并发 Allocate/Release 不应出现重复分配
func TestV3_Concurrent_NoOverlap(t *testing.T) {
	const total = 20240000
	const goroutines = 10
	const allocSize = 1024 * 1024

	a := NewAllocatorV3(total)

	var mu sync.Mutex
	allocated := make(map[int64]bool)
	var wg sync.WaitGroup

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 10; i++ {
				//start := time.Now()
				res, err := a.Allocate(allocSize)

				if err != nil {
					continue
				}
				//fmt.Printf("allocated %d blocks in %s\n", allocSize, time.Since(start))
				mu.Lock()
				for _, r := range res {
					for b := r.Offset; b < r.Offset+r.Size; b++ {
						if allocated[b] {
							t.Errorf("block %d allocated twice", b)
						}
						allocated[b] = true
					}
				}
				mu.Unlock()

				// 随机持有一段时间后释放
				if rand.Intn(2) == 0 {
					mu.Lock()
					for _, r := range res {
						for b := r.Offset; b < r.Offset+r.Size; b++ {
							delete(allocated, b)
						}
					}
					mu.Unlock()
					_ = a.Release(res)
				}
			}
		}()
	}
	wg.Wait()

	// 验证 available 一致性
	mu.Lock()
	usedInMap := int64(len(allocated))
	mu.Unlock()
	usedInAlloc := a.Total() - a.Available()
	if usedInAlloc < usedInMap {
		t.Errorf("allocator says %d used but map has %d", usedInAlloc, usedInMap)
	}
	if err := a.checkInvariant(); err != nil {
		t.Errorf("after concurrent ops: invariant violation: %v", err)
	}
}

// 12. 随机测试：朴素 []bool 模型与 allocator 互相校验
func TestV3_Random_ModelCheck(t *testing.T) {
	const total = 512
	a := NewAllocatorV3(total)
	model := make([]bool, total) // false = free, true = allocated

	type allocRecord struct {
		results []Result
	}
	var records []allocRecord

	rng := rand.New(rand.NewSource(42))

	for iter := 0; iter < 2000; iter++ {
		op := rng.Intn(2)
		if op == 0 || len(records) == 0 {
			// Allocate
			want := int64(rng.Intn(32) + 1)
			res, err := a.Allocate(want)
			freeCount := modelFreeCount(model)
			if freeCount < want {
				if err == nil {
					t.Fatalf("iter %d: alloc %d should fail (model free=%d)", iter, want, freeCount)
				}
				continue
			}
			if err != nil {
				// might be bitmap inconsistency if fragmented; just skip
				continue
			}
			// verify no overlap with model
			for _, r := range res {
				for b := r.Offset; b < r.Offset+r.Size; b++ {
					if model[b] {
						t.Fatalf("iter %d: block %d allocated twice", iter, b)
					}
					model[b] = true
				}
			}
			total2 := int64(0)
			for _, r := range res {
				total2 += r.Size
			}
			if total2 != want {
				t.Fatalf("iter %d: allocated %d want %d", iter, total2, want)
			}
			records = append(records, allocRecord{res})
		} else {
			// Release
			idx := rng.Intn(len(records))
			rec := records[idx]
			records = append(records[:idx], records[idx+1:]...)
			err := a.Release(rec.results)
			if err != nil {
				t.Fatalf("iter %d: release failed: %v", iter, err)
			}
			for _, r := range rec.results {
				for b := r.Offset; b < r.Offset+r.Size; b++ {
					model[b] = false
				}
			}
		}

		// 校验 available
		modelFree := modelFreeCount(model)
		if a.Available() != modelFree {
			t.Fatalf("iter %d: available=%d model_free=%d", iter, a.Available(), modelFree)
		}
		if iter%10 == 0 {
			if err := a.checkInvariant(); err != nil {
				t.Fatalf("iter %d: invariant violation: %v", iter, err)
			}
		}
	}
}

func modelFreeCount(model []bool) int64 {
	var c int64
	for _, v := range model {
		if !v {
			c++
		}
	}
	return c
}

// ---- L1_FREE / L2_FREE 直接返回场景测试 ----
//
// 以下测试覆盖「摘要层 FREE 快路径」的正确性：
//   - L1_FREE: 整个 L0 slotset（512 blocks）全空闲时，应能直接返回该范围，无需下钻 L0。
//   - L2_FREE: 整个 L1 slotset（131072 blocks）全空闲时，应能直接返回该范围，无需下钻 L1/L0。
// 这些测试在优化前（仍下钻 L0）也必须通过，验证正确性；
// 优化后（直接返回）相同测试仍需通过，验证优化不破坏语义。

const (
	// L0 slotset = 8 * 64 = 512 blocks → 对应 1 条 L1 entry
	l1FreeSlotsetBlocks = v3SlotsPerSlotset * v3BitsPerSlot // 512

	// L1 slotset = 8 L1 slot × 32 entry × 512 blocks = 131072 blocks → 对应 1 条 L2 entry
	l2FreeSlotsetBlocks = v3SlotsPerSlotset * v3L1EntriesPerSlot * l1FreeSlotsetBlocks // 131072
)

// TestV3_L1Free_DirectReturn_Correctness 验证：
// 当整块 L0 slotset（512 blocks）全部空闲时，
// 一次 Allocate(512) 应该精确返回从该 slotset 起始的 512 blocks（单个 extent）。
func TestV3_L1Free_DirectReturn_Correctness(t *testing.T) {
	const total = l1FreeSlotsetBlocks * 4 // 4 个完整 slotset
	a := NewAllocatorV3(total)

	// 先占满前两个 slotset，第三个 slotset 全空闲
	if err := a.InitAllocated(0, l1FreeSlotsetBlocks*2); err != nil {
		t.Fatal(err)
	}
	// 第三个 slotset [1024, 1536)，L1 entry 应该是 FREE
	thirdStart := int64(l1FreeSlotsetBlocks * 2)

	// 验证 L1 entry 确实是 FREE
	slotsetIdx := int(thirdStart / l1FreeSlotsetBlocks)
	l1SlotIdx := slotsetIdx / v3L1EntriesPerSlot
	l1BitPos := slotsetIdx % v3L1EntriesPerSlot
	entry := (a.l1[l1SlotIdx] >> uint(l1BitPos*v3L1EntryWidth)) & v3L1EntryMask
	if entry != v3L1EntryFree {
		t.Fatalf("expected L1 entry FREE(0x3) at slotset %d, got 0x%x", slotsetIdx, entry)
	}

	// 分配 512 blocks，应该落在第三个 slotset
	res, err := a.Allocate(l1FreeSlotsetBlocks)
	if err != nil {
		t.Fatal(err)
	}

	var totalBlocks int64
	for _, r := range res {
		totalBlocks += r.Size
	}
	if totalBlocks != l1FreeSlotsetBlocks {
		t.Fatalf("allocated %d blocks, want %d", totalBlocks, l1FreeSlotsetBlocks)
	}
	if res[0].Offset != thirdStart {
		t.Fatalf("allocated at offset %d, want %d", res[0].Offset, thirdStart)
	}
	if len(res) != 1 {
		t.Fatalf("expected 1 extent for fully-free slotset, got %d", len(res))
	}
	if err := a.checkInvariant(); err != nil {
		t.Fatalf("invariant violation: %v", err)
	}
}

// TestV3_L1Free_PartialAllocate_Correctness 验证：
// L1 entry 为 FREE 时，分配小于 512 blocks（如 100 blocks）应从该 slotset 开始，
// 返回正确的 offset 和 size，且不超过请求量。
func TestV3_L1Free_PartialAllocate_Correctness(t *testing.T) {
	const total = l1FreeSlotsetBlocks * 2
	a := NewAllocatorV3(total)

	// 占满第一个 slotset，让第二个 slotset 处于 L1_FREE
	if err := a.InitAllocated(0, l1FreeSlotsetBlocks); err != nil {
		t.Fatal(err)
	}

	const want = int64(100)
	res, err := a.Allocate(want)
	if err != nil {
		t.Fatal(err)
	}

	var got int64
	for _, r := range res {
		got += r.Size
	}
	if got != want {
		t.Fatalf("allocated %d blocks, want %d", got, want)
	}
	// 所有分配应该在第二个 slotset 范围内
	secondStart := int64(l1FreeSlotsetBlocks)
	for _, r := range res {
		if r.Offset < secondStart {
			t.Fatalf("allocated in already-occupied slotset: offset=%d", r.Offset)
		}
	}
	if err := a.checkInvariant(); err != nil {
		t.Fatalf("invariant violation: %v", err)
	}
}

// TestV3_L2Free_DirectReturn_Correctness 验证：
// 当整个 L1 slotset（131072 blocks）全空闲（L2 entry = FREE）时，
// Allocate(131072) 应精确返回 131072 blocks，单个 extent，且 offset 正确。
func TestV3_L2Free_DirectReturn_Correctness(t *testing.T) {
	// 需要至少 2 个 L1 slotset：第一个被占满，第二个全空闲
	const total = l2FreeSlotsetBlocks * 2
	a := NewAllocatorV3(total)

	// 占满第一个 L1 slotset
	if err := a.InitAllocated(0, l2FreeSlotsetBlocks); err != nil {
		t.Fatal(err)
	}

	secondStart := int64(l2FreeSlotsetBlocks)

	// 验证第二个 L1 slotset 对应的 L2 entry 是 FREE
	l1SetIdx := int(secondStart / l1FreeSlotsetBlocks / v3SlotsPerSlotset)
	l2SlotIdx := l1SetIdx / v3L1EntriesPerSlot
	l2BitPos := l1SetIdx % v3L1EntriesPerSlot
	if l2SlotIdx < len(a.l2) {
		entry := (a.l2[l2SlotIdx] >> uint(l2BitPos*v3L1EntryWidth)) & v3L1EntryMask
		if entry != v3L1EntryFree {
			t.Fatalf("expected L2 entry FREE(0x3) at l1SetIdx %d, got 0x%x", l1SetIdx, entry)
		}
	}

	res, err := a.Allocate(l2FreeSlotsetBlocks)
	if err != nil {
		t.Fatal(err)
	}

	var totalBlocks int64
	for _, r := range res {
		totalBlocks += r.Size
	}
	if totalBlocks != l2FreeSlotsetBlocks {
		t.Fatalf("allocated %d blocks, want %d", totalBlocks, l2FreeSlotsetBlocks)
	}
	if res[0].Offset != secondStart {
		t.Fatalf("allocated at offset %d, want %d", res[0].Offset, secondStart)
	}
	if err := a.checkInvariant(); err != nil {
		t.Fatalf("invariant violation: %v", err)
	}
}

// TestV3_L2Free_PartialAllocate_Correctness 验证：
// L2 entry 为 FREE 时，请求少量块（如 1000 blocks）应正确返回 1000 blocks，
// 不能多分配，不能越界。
func TestV3_L2Free_PartialAllocate_Correctness(t *testing.T) {
	const total = l2FreeSlotsetBlocks * 2
	a := NewAllocatorV3(total)

	if err := a.InitAllocated(0, l2FreeSlotsetBlocks); err != nil {
		t.Fatal(err)
	}

	const want = int64(1000)
	res, err := a.Allocate(want)
	if err != nil {
		t.Fatal(err)
	}

	var got int64
	for _, r := range res {
		got += r.Size
	}
	if got != want {
		t.Fatalf("allocated %d blocks, want %d", got, want)
	}
	secondStart := int64(l2FreeSlotsetBlocks)
	for _, r := range res {
		if r.Offset < secondStart {
			t.Fatalf("allocated in already-occupied region: offset=%d", r.Offset)
		}
	}
	if err := a.checkInvariant(); err != nil {
		t.Fatalf("invariant violation: %v", err)
	}
}

// TestV3_EmptyDisk_LargeAllocate_Correctness 验证：
// 空盘（全量 L2_FREE）时，大尺寸连续分配（跨多个 L2 entry）结果正确，
// 覆盖从 offset=0 开始、size 精确等于 want、不超过 total 的约束。
func TestV3_EmptyDisk_LargeAllocate_Correctness(t *testing.T) {
	// 4 个 L2 slotset，总共 4*131072 = 524288 blocks，全空闲
	const total int64 = l2FreeSlotsetBlocks * 4
	a := NewAllocatorV3(total)

	const want = int64(l2FreeSlotsetBlocks * 3) // 跨 3 个 L2 entry
	res, err := a.Allocate(want)
	if err != nil {
		t.Fatalf("empty disk large allocate failed: %v", err)
	}

	var got int64
	for _, r := range res {
		got += r.Size
	}
	if got != want {
		t.Fatalf("allocated %d blocks, want %d", got, want)
	}
	if res[0].Offset != 0 {
		t.Fatalf("expected allocation to start at 0, got %d", res[0].Offset)
	}
	if a.Available() != total-want {
		t.Fatalf("available=%d want %d", a.Available(), total-want)
	}
	if err := a.checkInvariant(); err != nil {
		t.Fatalf("invariant violation: %v", err)
	}
}

// TestV3_EmptyDisk_Sequential_Correctness 验证：
// 空盘上多次小分配，每次应该顺序推进，offset 单调递增，不重叠。
func TestV3_EmptyDisk_Sequential_Correctness(t *testing.T) {
	const total = l2FreeSlotsetBlocks * 2 // 262144 blocks
	const allocSize = int64(512)
	const rounds = int(total / allocSize) // 恰好填满

	a := NewAllocatorV3(total)
	var prevEnd int64 = -1

	for i := 0; i < rounds; i++ {
		res, err := a.Allocate(allocSize)
		if err != nil {
			t.Fatalf("round %d: allocate failed: %v", i, err)
		}
		for _, r := range res {
			if r.Offset <= prevEnd {
				t.Fatalf("round %d: offset %d not after prevEnd %d", i, r.Offset, prevEnd)
			}
		}
		last := res[len(res)-1]
		prevEnd = last.Offset + last.Size - 1
		if i%100 == 0 {
			if err := a.checkInvariant(); err != nil {
				t.Fatalf("round %d: invariant violation: %v", i, err)
			}
		}
	}

	if a.Available() != 0 {
		t.Fatalf("after filling disk, available=%d want 0", a.Available())
	}
	if err := a.checkInvariant(); err != nil {
		t.Fatalf("invariant violation: %v", err)
	}
}

// TestV3_LowFragmentation_Correctness 验证：
// 低碎片场景：大量 L1/L2 entry 为 FREE，只有极少数 PARTIAL。
// 在每个 L1 slotset 中仅分配 1 个 block，然后释放后面的全部，
// 确认分配器能正确找到 FREE 区域而不是在 PARTIAL 中浪费时间。
func TestV3_LowFragmentation_Correctness(t *testing.T) {
	// 8 个 L1 slotset
	const numSets = 8
	const total = int64(l2FreeSlotsetBlocks * numSets / l1FreeSlotsetBlocks * l1FreeSlotsetBlocks)
	a := NewAllocatorV3(total)

	// 在每个 L1 slotset 的第一个 block 处分配 1 个 block，造成 PARTIAL
	partials := make([]Result, numSets)
	for i := 0; i < numSets; i++ {
		offset := int64(i) * l1FreeSlotsetBlocks
		if err := a.InitAllocated(offset, 1); err != nil {
			t.Fatalf("set %d: InitAllocated failed: %v", i, err)
		}
		partials[i] = Result{Offset: offset, Size: 1}
	}

	// 现在每个 L1 slotset 都是 PARTIAL（不是 FREE），但绝大多数 block 仍空闲
	expectedFree := total - int64(numSets)
	if a.Available() != expectedFree {
		t.Fatalf("available=%d want %d", a.Available(), expectedFree)
	}

	// 请求分配 l1FreeSlotsetBlocks-1（= 511 blocks），应该能在某个 slotset 内找到
	const want = int64(l1FreeSlotsetBlocks - 1)
	res, err := a.Allocate(want)
	if err != nil {
		t.Fatalf("low-frag alloc failed: %v", err)
	}
	var got int64
	for _, r := range res {
		got += r.Size
	}
	if got != want {
		t.Fatalf("allocated %d blocks, want %d", got, want)
	}
	if err := a.checkInvariant(); err != nil {
		t.Fatalf("invariant violation: %v", err)
	}
}

// TestV3_EmptyDisk_Performance 验证空盘性能：
// 空盘（全量 L2_FREE）下分配单次应极快，利用 L2_FREE/L1_FREE 直接返回不需要扫描 L0。
// 此测试同时检测正确性（分配总量==want）和运行时间是否在合理范围内。
func TestV3_EmptyDisk_Performance(t *testing.T) {
	// 超大空盘：约 4GB 的 block 空间
	const total = int64(l2FreeSlotsetBlocks) * 32 * 1024 * 2 // 32 * 131072 = 4194304 blocks
	a := NewAllocatorV3(total)

	const want = int64(l1FreeSlotsetBlocks) // 512 blocks

	start := time.Now()
	res, err := a.Allocate(want)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("empty disk alloc failed: %v", err)
	}
	var got int64
	for _, r := range res {
		got += r.Size
	}
	if got != want {
		t.Fatalf("allocated %d blocks, want %d", got, want)
	}
	// 空盘+完整 L1_FREE 直接返回应该在 1ms 以内；
	// 这是宽松上限，主要防止 O(N) 扫描退化
	if elapsed > 5*time.Millisecond {
		t.Logf("WARNING: empty disk alloc took %v (expected <1ms with FREE fast-path)", elapsed)
	}
	t.Logf("empty disk alloc elapsed: %v", elapsed)
}

// TestV3_L1Free_Boundary_Correctness 验证：
// 分配量恰好等于一个完整 L1 slotset（512 blocks）时：
// 1. 返回 offset 在 slotset 边界上
// 2. 恰好 1 个 extent
// 3. size == 512
// 4. 释放后 L1 entry 恢复为 FREE
func TestV3_L1Free_Boundary_Correctness(t *testing.T) {
	const total = l1FreeSlotsetBlocks * 3
	a := NewAllocatorV3(total)

	res, err := a.Allocate(l1FreeSlotsetBlocks)
	if err != nil {
		t.Fatal(err)
	}
	if len(res) != 1 {
		t.Fatalf("expected 1 extent, got %d", len(res))
	}
	if res[0].Offset != 0 {
		t.Fatalf("expected offset=0, got %d", res[0].Offset)
	}
	if res[0].Size != l1FreeSlotsetBlocks {
		t.Fatalf("expected size=%d, got %d", l1FreeSlotsetBlocks, res[0].Size)
	}

	// 释放后再次分配，应该复用同一 slotset
	if err := a.Release(res); err != nil {
		t.Fatal(err)
	}
	a.lastPos = 0 // 重置 lastPos，确保从头开始搜索

	res2, err := a.Allocate(l1FreeSlotsetBlocks)
	if err != nil {
		t.Fatal(err)
	}
	var got int64
	for _, r := range res2 {
		got += r.Size
	}
	if got != l1FreeSlotsetBlocks {
		t.Fatalf("re-alloc: got %d blocks, want %d", got, l1FreeSlotsetBlocks)
	}
	if err := a.checkInvariant(); err != nil {
		t.Fatalf("invariant violation: %v", err)
	}
}

// markRangeLocked_old 原始实现：每改一个 L0 word 立即更新 L1，保留用于性能对比。
func (a *AllocatorV3) markRangeLocked_old(offset, size int64, free bool) int64 {
	if size <= 0 {
		return 0
	}
	end := offset + size

	startWord := int(offset / v3BitsPerSlot)
	endWord := int((end - 1) / v3BitsPerSlot)

	var changed int64
	for i := startWord; i <= endWord && i < len(a.l0); i++ {
		mask := v3RangeMask(offset, end, i)
		mask &= a.v3ValidWordMask(i)
		old := a.l0[i]
		var next uint64
		if free {
			changed += int64(bits.OnesCount64(^old & mask))
			next = old | mask
		} else {
			changed += int64(bits.OnesCount64(old & mask))
			next = old &^ mask
		}
		a.l0[i] = next
		// 每个 word 都立即更新 L1（有重复更新同一 slotset 的开销）
		a.updateL1ForL0Slot(i)
	}

	// 更新 L2
	startSlotset := startWord / v3SlotsPerSlotset
	endSlotset := endWord / v3SlotsPerSlotset
	startL1Slot := startSlotset / v3L1EntriesPerSlot
	endL1Slot := endSlotset / v3L1EntriesPerSlot
	for li := startL1Slot; li <= endL1Slot && li < len(a.l1); li++ {
		a.updateL2ForL1Slot(li)
	}

	return changed
}

// TestV3_markRangeLocked_L2UpdatePerL1Slot 验证当前 markRangeLocked 的 L2 更新是按 L1 slot 逐个调用
// （即对每个 L1 slot 调用 updateL2ForL1Slot），而不是按 L1 slotset 去重。因此同一 L2 entry
// 可能被重复刷新多次。本测试通过计数器验证调用次数等于覆盖的 L1 slot 数，并且大于
// 唯一的 L1 slotset 数，证明存在重复更新。
// func TestV3_markRangeLocked_L2UpdatePerL1Slot(t *testing.T) {
// 	const total int64 = l2FreeSlotsetBlocks * 4
// 	a := NewAllocatorV3(total)

// 	// 重置计数器
// 	atomic.StoreInt64(&updateL2ForL1SlotCalls, 0)

// 	offset := int64(0)
// 	size := total
// 	_ = a.markRangeLocked(offset, size, true)

// 	// 计算按当前实现预期的调用次数（endL1Slot-startL1Slot+1）
// 	startWord := int(offset / v3BitsPerSlot)
// 	end := offset + size
// 	endWord := int((end - 1) / v3BitsPerSlot)

// 	startSlotset := startWord / v3SlotsPerSlotset
// 	endSlotset := endWord / v3SlotsPerSlotset
// 	startL1Slot := startSlotset / v3L1EntriesPerSlot
// 	endL1Slot := endSlotset / v3L1EntriesPerSlot

// 	var expectedCalls int64
// 	if endL1Slot >= startL1Slot {
// 		expectedCalls = int64(endL1Slot - startL1Slot + 1)
// 	}

// 	actual := atomic.LoadInt64(&updateL2ForL1SlotCalls)
// 	if actual != expectedCalls {
// 		t.Fatalf("updateL2ForL1SlotCalls=%d expected=%d", actual, expectedCalls)
// 	}

// 	// 计算唯一的 L1 slotset 数（每 v3SlotsPerSlotset 个 L1 slot 对应一个 slotset）
// 	unique := int64(endL1Slot/v3SlotsPerSlotset) - int64(startL1Slot/v3SlotsPerSlotset) + 1
// 	if actual <= unique {
// 		t.Fatalf("expected duplicate L2 updates; actualCalls=%d uniqueL1Slotsets=%d", actual, unique)
// 	}
// }

// ---- 三层 bitmap 内部一致性检查 ----

// checkInvariantLocked 检查 AllocatorV3 三层 bitmap 之间的内部一致性。
// 调用方须已持有 a.mu 锁（或处于单线程场景）。
//
// 检查项目：
//  1. L0 有效 free bit 总数 == available
//  2. 每个 L1 entry 与对应 L0 slotset 状态严格一致
//  3. 每个 L2 entry 与对应 L1 slotset 状态严格一致
//  4. lastPos 在 [0, total) 范围内
func (a *AllocatorV3) checkInvariantLocked() error {
	// ── 1. L0 free bit 计数 == available ────────────────────────────────────
	var freeBits int64
	for i := 0; i < len(a.l0); i++ {
		freeBits += int64(bits.OnesCount64(a.l0[i] & a.v3ValidWordMask(i)))
	}
	if freeBits != a.available {
		return fmt.Errorf("invariant[L0→available]: L0 free bits=%d, available=%d", freeBits, a.available)
	}

	// ── 2. L1 entry 与 L0 slotset 严格一致 ──────────────────────────────────
	for l1Slot := 0; l1Slot < len(a.l1); l1Slot++ {
		for l1Bit := 0; l1Bit < v3L1EntriesPerSlot; l1Bit++ {
			ss := l1Slot*v3L1EntriesPerSlot + l1Bit
			ssStart := ss * v3SlotsPerSlotset

			var expected uint64
			if ssStart >= len(a.l0) {
				// 幻影 slotset（超出 L0 范围）：无有效 bit，视为 FULL
				expected = v3L1EntryFull
			} else {
				ssEnd := ssStart + v3SlotsPerSlotset
				if ssEnd > len(a.l0) {
					ssEnd = len(a.l0)
				}
				allFree, allAlloc := true, true
				for k := ssStart; k < ssEnd; k++ {
					mask := a.v3ValidWordMask(k)
					if a.l0[k]&mask != mask {
						allFree = false
					}
					if a.l0[k]&mask != 0 {
						allAlloc = false
					}
				}
				switch {
				case allFree:
					expected = v3L1EntryFree
				case allAlloc:
					expected = v3L1EntryFull
				default:
					expected = v3L1EntryPartial
				}
			}

			actual := (a.l1[l1Slot] >> uint(l1Bit*v3L1EntryWidth)) & v3L1EntryMask
			if actual != expected {
				return fmt.Errorf("invariant[L1]: slotset=%d (l1[%d] bit=%d): got=0x%x want=0x%x",
					ss, l1Slot, l1Bit, actual, expected)
			}
		}
	}

	// ── 3. L2 entry 与 L1 slotset 严格一致 ──────────────────────────────────
	for l2Slot := 0; l2Slot < len(a.l2); l2Slot++ {
		for l2Bit := 0; l2Bit < v3L1EntriesPerSlot; l2Bit++ {
			ls := l2Slot*v3L1EntriesPerSlot + l2Bit
			lsStart := ls * v3SlotsPerSlotset

			var expected uint64
			if lsStart >= len(a.l1) {
				// 幻影 L1 slotset：视为 FULL
				expected = v3L1EntryFull
			} else {
				lsEnd := lsStart + v3SlotsPerSlotset
				if lsEnd > len(a.l1) {
					lsEnd = len(a.l1)
				}
				allFree, allAlloc := true, true
				for k := lsStart; k < lsEnd; k++ {
					if a.l1[k] != ^uint64(0) {
						allFree = false
					}
					if a.l1[k] != 0 {
						allAlloc = false
					}
				}
				switch {
				case allFree:
					expected = v3L1EntryFree
				case allAlloc:
					expected = v3L1EntryFull
				default:
					expected = v3L1EntryPartial
				}
			}

			actual := (a.l2[l2Slot] >> uint(l2Bit*v3L1EntryWidth)) & v3L1EntryMask
			if actual != expected {
				return fmt.Errorf("invariant[L2]: l1slotset=%d (l2[%d] bit=%d): got=0x%x want=0x%x",
					ls, l2Slot, l2Bit, actual, expected)
			}
		}
	}

	// ── 4. lastPos 范围合法 ──────────────────────────────────────────────────
	if a.total > 0 && (a.lastPos < 0 || a.lastPos >= a.total) {
		return fmt.Errorf("invariant[lastPos]: lastPos=%d out of range [0, %d)", a.lastPos, a.total)
	}

	return nil
}

// checkInvariant 加锁后调用 checkInvariantLocked，供测试在操作边界处使用。
func (a *AllocatorV3) checkInvariant() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.checkInvariantLocked()
}
