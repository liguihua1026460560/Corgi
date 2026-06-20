package fs

// allocator_v3_bench_test.go — AllocatorV3 性能基准测试
//
// 覆盖场景：
//   1. 空盘分配（全量 L2_FREE / L1_FREE 快路径）
//   2. 满盘后释放再分配（测试 Release + re-Allocate 路径）
//   3. 不同分配粒度（1 / 64 / 512 / 4096 / 131072 blocks）
//   4. 不同磁盘大小（512K / 16M / 1G / 64G blocks）
//   5. 高碎片场景（交替分配释放，产生大量 PARTIAL entry）
//   6. 顺序填满场景（持续分配直到磁盘满）
//   7. 并发分配（多 goroutine 竞争同一 allocator）
//   8. New + 销毁（纯初始化开销）
//   9. markRange 内部热路径（InitAllocated / InitFree）
//  10. 释放大 extent

import (
	"math/rand"
	"sync"
	"testing"
)

// ── 辅助：磁盘大小常量 ──────────────────────────────────────────────────────

const (
	bench512K  = int64(512 * 1024)              // 524 288 blocks
	bench16M   = int64(16 * 1024 * 1024)        // 16 777 216 blocks
	bench1G    = int64(1024 * 1024 * 1024)      // 1 073 741 824 blocks
	bench64G   = int64(64 * 1024 * 1024 * 1024) // 68 719 476 736 blocks
	benchAlloc = int64(v3BitsPerSlotset)        // 512 blocks（一个 L1 entry 单位）
)

// ── 1. 空盘分配：每次分配一个 L1 slotset（512 blocks），全程 L1_FREE 快路径 ───

func BenchmarkV3_EmptyDisk_Alloc512(b *testing.B) {
	benchEmptyDiskAlloc(b, bench16M, 512)
}

func BenchmarkV3_EmptyDisk_Alloc1(b *testing.B) {
	benchEmptyDiskAlloc(b, bench16M, 1)
}

func BenchmarkV3_EmptyDisk_Alloc64(b *testing.B) {
	benchEmptyDiskAlloc(b, bench16M, 64)
}

func BenchmarkV3_EmptyDisk_Alloc4096(b *testing.B) {
	benchEmptyDiskAlloc(b, bench16M, 4096)
}

func BenchmarkV3_EmptyDisk_AllocL2Unit(b *testing.B) {
	// 每次分配 1 个完整 L1 slotset（131072 blocks），测 L2_FREE 快路径
	benchEmptyDiskAlloc(b, bench1G, 131072)
}

func benchEmptyDiskAlloc(b *testing.B, total, allocSize int64) {
	b.Helper()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		a := NewAllocatorV3(total)
		b.StartTimer()

		left := total
		for left >= allocSize {
			res, err := a.Allocate(allocSize)
			if err != nil {
				break
			}
			left -= sumResults(res)
		}
	}
}

// ── 2. 超大空盘单次分配（测 L2_FREE 快路径延迟）────────────────────────────

func BenchmarkV3_EmptyDisk_SingleAlloc_1G(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		a := NewAllocatorV3(bench1G)
		b.StartTimer()
		res, _ := a.Allocate(512)
		_ = res
	}
}

func BenchmarkV3_EmptyDisk_SingleAlloc_64G(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		a := NewAllocatorV3(bench64G)
		b.StartTimer()
		res, _ := a.Allocate(512)
		_ = res
	}
}

// ── 3. 顺序填满再释放再填满（稳态吞吐） ────────────────────────────────────

func BenchmarkV3_FillAndRefill_512K(b *testing.B) {
	benchFillAndRefill(b, bench512K, 512)
}

func BenchmarkV3_FillAndRefill_16M(b *testing.B) {
	benchFillAndRefill(b, bench16M, 512)
}

func benchFillAndRefill(b *testing.B, total, allocSize int64) {
	b.Helper()
	b.ReportAllocs()
	a := NewAllocatorV3(total)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		// 先填满
		var extents [][]Result
		for a.Available() >= allocSize {
			res, err := a.Allocate(allocSize)
			if err != nil {
				break
			}
			extents = append(extents, res)
		}
		b.StartTimer()

		// 再全部释放
		for _, res := range extents {
			_ = a.Release(res)
		}
		extents = extents[:0]

		// 再次填满
		for a.Available() >= allocSize {
			res, err := a.Allocate(allocSize)
			if err != nil {
				break
			}
			extents = append(extents, res)
		}
	}
}

// ── 4. 单次 Allocate 各粒度延迟（空盘，仅测一次 Allocate） ─────────────────

func BenchmarkV3_SingleAllocLatency_1block(b *testing.B) {
	benchSingleAllocLatency(b, bench16M, 1)
}

func BenchmarkV3_SingleAllocLatency_64blocks(b *testing.B) {
	benchSingleAllocLatency(b, bench16M, 64)
}

func BenchmarkV3_SingleAllocLatency_512blocks(b *testing.B) {
	benchSingleAllocLatency(b, bench16M, 512)
}

// func BenchmarkV3_SingleAllocLatency_131072blocks(b *testing.B) {
// 	benchSingleAllocLatency(b, bench1G, v3BitsPerL1Slotset)
// }

func benchSingleAllocLatency(b *testing.B, total, allocSize int64) {
	b.Helper()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		a := NewAllocatorV3(total)
		b.StartTimer()
		res, _ := a.Allocate(allocSize)
		_ = res
	}
}

// ── 5. 高碎片场景：交替分配/释放 1 block，制造大量 PARTIAL entry ───────────

func BenchmarkV3_HighFrag_AllocRelease1(b *testing.B) {
	const total = bench16M
	b.ReportAllocs()

	// 预先在每个 L0 slotset 分配 1 个 block（前 1024 个 slotset）
	a := NewAllocatorV3(total)
	numSlotsets := int(total / v3BitsPerSlotset)
	for i := 0; i < numSlotsets; i++ {
		_ = a.InitAllocated(int64(i)*v3BitsPerSlotset, 1)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// 从碎片化的分配器中再分配 1 block
		res, err := a.Allocate(1)
		if err != nil {
			// 如果满了，先重置
			b.StopTimer()
			a = NewAllocatorV3(total)
			for j := 0; j < numSlotsets; j++ {
				_ = a.InitAllocated(int64(j)*v3BitsPerSlotset, 1)
			}
			b.StartTimer()
			res, _ = a.Allocate(1)
		}
		if res != nil {
			_ = a.Release(res)
		}
	}
}

// ── 6. Release 大 extent 性能 ───────────────────────────────────────────────

func BenchmarkV3_Release_LargeExtent_L1Unit(b *testing.B) {
	benchRelease(b, bench16M, v3BitsPerSlotset) // 512 blocks
}

// func BenchmarkV3_Release_LargeExtent_L2Unit(b *testing.B) {
// 	benchRelease(b, bench1G, v3BitsPerL1Slotset) // 131072 blocks
// }

func benchRelease(b *testing.B, total, allocSize int64) {
	b.Helper()
	b.ReportAllocs()
	a := NewAllocatorV3(total)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		res, err := a.Allocate(allocSize)
		if err != nil {
			// 空间不足则重建
			a = NewAllocatorV3(total)
			res, _ = a.Allocate(allocSize)
		}
		b.StartTimer()
		_ = a.Release(res)
	}
}

// ── 7. 并发 Allocate（多 goroutine 竞争） ───────────────────────────────────

func BenchmarkV3_Concurrent_Alloc_4G(b *testing.B) {
	benchConcurrentAlloc(b, bench1G*4, 512, 8)
}

func BenchmarkV3_Concurrent_Alloc_Goroutines4(b *testing.B) {
	benchConcurrentAlloc(b, bench1G, 512, 4)
}

func BenchmarkV3_Concurrent_Alloc_Goroutines16(b *testing.B) {
	benchConcurrentAlloc(b, bench1G, 512, 16)
}

func BenchmarkV3_Concurrent_Alloc_Goroutines64(b *testing.B) {
	benchConcurrentAlloc(b, bench1G, 512, 64)
}

func benchConcurrentAlloc(b *testing.B, total, allocSize int64, goroutines int) {
	b.Helper()
	b.ReportAllocs()
	a := NewAllocatorV3(total)
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			res, err := a.Allocate(allocSize)
			if err == nil {
				_ = a.Release(res)
			}
		}
	})
	_ = goroutines
}

// ── 8. 并发 Allocate + Release 混合（真实工作负载模拟） ────────────────────

func BenchmarkV3_Concurrent_Mixed_8G(b *testing.B) {
	const total = bench1G * 8
	const allocSize = int64(512)
	b.ReportAllocs()
	a := NewAllocatorV3(total)

	var pool sync.Pool
	pool.New = func() interface{} { return []Result(nil) }

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		rng := rand.New(rand.NewSource(42))
		var held [][]Result
		for pb.Next() {
			if len(held) > 0 && rng.Intn(2) == 0 {
				idx := rng.Intn(len(held))
				_ = a.Release(held[idx])
				held = append(held[:idx], held[idx+1:]...)
			} else {
				res, err := a.Allocate(allocSize)
				if err == nil {
					held = append(held, res)
				}
			}
		}
		// 清理持有的 extents
		for _, res := range held {
			_ = a.Release(res)
		}
	})
}

// ── 9. NewAllocatorV3 初始化开销 ────────────────────────────────────────────

func BenchmarkV3_New_512K(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		a := NewAllocatorV3(bench512K)
		_ = a
	}
}

func BenchmarkV3_New_16M(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		a := NewAllocatorV3(bench16M)
		_ = a
	}
}

func BenchmarkV3_New_1G(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		a := NewAllocatorV3(bench1G)
		_ = a
	}
}

func BenchmarkV3_New_64G(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		a := NewAllocatorV3(bench64G)
		_ = a
	}
}

// ── 10. InitAllocated / InitFree 热路径（markRange 内部） ───────────────────

func BenchmarkV3_InitAllocated_SmallRange(b *testing.B) {
	a := NewAllocatorV3(bench16M)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		off := int64(i%int(bench16M/64)) * 64
		_ = a.InitAllocated(off, 64)
	}
}

func BenchmarkV3_InitFree_SmallRange(b *testing.B) {
	a := NewAllocatorV3(bench16M)
	// 先全部占满
	_ = a.InitAllocated(0, bench16M)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		off := int64(i%int(bench16M/64)) * 64
		_ = a.InitFree(off, 64)
	}
}

// ── 11. 随机 offset 分配（模拟随机 IO 工作负载） ────────────────────────────

func BenchmarkV3_RandomAllocFree_512K(b *testing.B) {
	benchRandomAllocFree(b, bench512K, 512)
}

func BenchmarkV3_RandomAllocFree_16M(b *testing.B) {
	benchRandomAllocFree(b, bench16M, 512)
}

func benchRandomAllocFree(b *testing.B, total, allocSize int64) {
	b.Helper()
	b.ReportAllocs()
	a := NewAllocatorV3(total)
	rng := rand.New(rand.NewSource(12345))

	// 先填半盘
	numAlloc := int(total / allocSize / 2)
	var allocs [][]Result
	for i := 0; i < numAlloc; i++ {
		res, err := a.Allocate(allocSize)
		if err != nil {
			break
		}
		allocs = append(allocs, res)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// 随机释放一个已分配的 extent
		if len(allocs) > 0 {
			idx := rng.Intn(len(allocs))
			_ = a.Release(allocs[idx])
			allocs = append(allocs[:idx], allocs[idx+1:]...)
		}
		// 重新分配
		res, err := a.Allocate(allocSize)
		if err == nil {
			allocs = append(allocs, res)
		}
	}
}

// ── 12. 半满磁盘分配（PARTIAL entry 为主） ──────────────────────────────────

func BenchmarkV3_HalfFull_Alloc512(b *testing.B) {
	benchHalfFullAlloc(b, bench16M, 512)
}

func BenchmarkV3_HalfFull_Alloc1(b *testing.B) {
	benchHalfFullAlloc(b, bench16M, 1)
}

func benchHalfFullAlloc(b *testing.B, total, allocSize int64) {
	b.Helper()
	b.ReportAllocs()
	a := NewAllocatorV3(total)

	// 填半盘
	half := total / 2
	_ = a.InitAllocated(0, half)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		if a.Available() < allocSize {
			// 重置
			a = NewAllocatorV3(total)
			_ = a.InitAllocated(0, half)
		}
		b.StartTimer()
		res, err := a.Allocate(allocSize)
		if err == nil {
			_ = a.Release(res)
		}
	}
}

// ── 13. 分配后立即释放（alloc+release 往返延迟） ────────────────────────────

func BenchmarkV3_AllocRelease_RoundTrip_512(b *testing.B) {
	a := NewAllocatorV3(bench16M)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		res, err := a.Allocate(512)
		if err != nil {
			b.StopTimer()
			a = NewAllocatorV3(bench16M)
			b.StartTimer()
			res, _ = a.Allocate(512)
		}
		_ = a.Release(res)
	}
}

func BenchmarkV3_AllocRelease_RoundTrip_1(b *testing.B) {
	a := NewAllocatorV3(bench16M)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		res, err := a.Allocate(1)
		if err != nil {
			b.StopTimer()
			a = NewAllocatorV3(bench16M)
			b.StartTimer()
			res, _ = a.Allocate(1)
		}
		_ = a.Release(res)
	}
}

// ── 14. 磁盘规模对单次分配延迟的影响（L2_FREE 快路径不应随规模线性增长） ────

func BenchmarkV3_ScaleVsLatency_512K(b *testing.B) {
	benchScaleLatency(b, bench512K)
}

func BenchmarkV3_ScaleVsLatency_16M(b *testing.B) {
	benchScaleLatency(b, bench16M)
}

func BenchmarkV3_ScaleVsLatency_1G(b *testing.B) {
	benchScaleLatency(b, bench1G)
}

func BenchmarkV3_ScaleVsLatency_64G(b *testing.B) {
	benchScaleLatency(b, bench64G)
}

func benchScaleLatency(b *testing.B, total int64) {
	b.Helper()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		a := NewAllocatorV3(total)
		b.StartTimer()
		res, _ := a.Allocate(512)
		_ = res
	}
}

// ── 15. 近满盘分配（触发两遍扫描 fallback） ─────────────────────────────────

func BenchmarkV3_NearFull_Alloc512(b *testing.B) {
	const total = bench512K
	const allocSize = int64(512)
	b.ReportAllocs()

	a := NewAllocatorV3(total)
	// 填到只剩 1 个 slotset
	leaveBlocks := allocSize
	toFill := total - leaveBlocks
	_ = a.InitAllocated(0, toFill)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		if a.Available() < allocSize {
			a = NewAllocatorV3(total)
			_ = a.InitAllocated(0, toFill)
		}
		b.StartTimer()
		res, err := a.Allocate(allocSize)
		if err == nil {
			b.StopTimer()
			_ = a.Release(res)
			b.StartTimer()
		}
	}
}

// ── 辅助函数 ────────────────────────────────────────────────────────────────

func sumResults(res []Result) int64 {
	var s int64
	for _, r := range res {
		s += r.Size
	}
	return s
}

// ── 16. markRangeLocked_old vs markRangeLocked 直接对比 ────────────────────
//
// 两个 benchmark 使用完全相同的操作序列，唯一区别是调用旧版（每 word 刷 L1）
// 还是新版（批量 L0 + 按 slotset 去重刷 L1）。
// 测试场景：大范围连续 mark（最能体现去重收益）。

// BenchmarkV3_MarkRange_Old 测试原始实现：每修改一个 L0 word 就调用 updateL1。
// 对于跨 N 个 slotset 的大范围，会重复刷新 L1 entry N*8 次（每 slotset 8 次）。
func BenchmarkV3_MarkRange_Old(b *testing.B) {
	benchMarkRange(b, true)
}

// BenchmarkV3_MarkRange_New 测试优化实现：先批量改 L0，再按 slotset 去重刷 L1。
// 对于跨 N 个 slotset 的大范围，L1 只刷 N 次，L2 只刷对应的 L1 slotset 数。
func BenchmarkV3_MarkRange_New(b *testing.B) {
	benchMarkRange(b, false)
}

// benchMarkRange 是上面两个 benchmark 的共享驱动：
//   - useOld=true  → 调用 markRangeLocked_old
//   - useOld=false → 调用 markRangeLocked
//
// 操作序列：在一个 16M blocks 的 allocator 上，
// 对 [0, 16M) 整段交替执行 mark-allocated / mark-free，
// 每次操作跨越全部 L0/L1/L2，充分放大去重收益。
func benchMarkRange(b *testing.B, useOld bool) {
	b.Helper()
	const total = bench16M
	a := NewAllocatorV3(total)
	b.ReportAllocs()
	b.ResetTimer()

	free := false
	for i := 0; i < b.N; i++ {
		if useOld {
			_ = a.markRangeLocked_old(0, total, free)
		} else {
			_ = a.markRangeLocked(0, total, free)
		}
		free = !free
	}
}

// BenchmarkV3_MarkRange_Old_SmallSize 测试小范围（单 slotset 512 blocks）旧版开销。
// 小范围下旧版与新版的差异应较小（最多 8 次重复 vs 1 次），作为基线验证。
func BenchmarkV3_MarkRange_Old_SmallSize(b *testing.B) {
	benchMarkRangeSize(b, true, v3BitsPerSlotset)
}

// BenchmarkV3_MarkRange_New_SmallSize 测试小范围（单 slotset 512 blocks）新版开销。
func BenchmarkV3_MarkRange_New_SmallSize(b *testing.B) {
	benchMarkRangeSize(b, false, v3BitsPerSlotset)
}

// BenchmarkV3_MarkRange_Old_MediumSize 测试中等范围（一个 L1 slotset = 131072 blocks）旧版。
func BenchmarkV3_MarkRange_Old_MediumSize(b *testing.B) {
	benchMarkRangeSize(b, true, v3BitsPerL1Slotset)
}

// BenchmarkV3_MarkRange_New_MediumSize 测试中等范围（一个 L1 slotset = 131072 blocks）新版。
func BenchmarkV3_MarkRange_New_MediumSize(b *testing.B) {
	benchMarkRangeSize(b, false, v3BitsPerL1Slotset)
}

func benchMarkRangeSize(b *testing.B, useOld bool, size int64) {
	b.Helper()
	const total = bench1G
	a := NewAllocatorV3(total)
	b.ReportAllocs()
	b.ResetTimer()

	free := false
	for i := 0; i < b.N; i++ {
		if useOld {
			_ = a.markRangeLocked_old(0, size, free)
		} else {
			_ = a.markRangeLocked(0, size, free)
		}
		free = !free
	}
}
