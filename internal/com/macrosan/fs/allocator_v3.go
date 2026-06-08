package fs

// allocator_v3.go — 三层 Bitmap 分配器，参考 Ceph fastbmap_allocator_impl.cc
//
// 设计概述：
//   - L0: 细粒度 bitmap，每个 bit 表示一个 block（1 表示空闲，0 表示已分配）。
//   - L1: 摘要层，每 2 bit 描述 L0 中一个 slotset（8 个 uint64 = 512 个 block）的状态：
//         L1_FREE(0x3)    = 整个 slotset 全空闲
//         L1_PARTIAL(0x1) = 部分空闲
//         L1_FULL(0x0)    = 全部已分配
//   - L2: 更高层摘要，每 2 bit 描述 L1 中一个 slotset 的状态，用于快速跳过大段已满区域。
//
// 用法示例：
//   alloc := NewAllocatorV3(totalBlocks)
//   results, err := alloc.Allocate(want)   // 返回 []Result{Offset, Size}
//   err = alloc.Release(results)
//   err = alloc.InitAllocated(offset, size) // 标记已占用（恢复时使用）
//   err = alloc.InitFree(offset, size)      // 标记为空闲（恢复时使用）
//   free := alloc.Available()
//   total := alloc.Total()

import (
	"fmt"
	"math/bits"
	"sync"
)

const (
	v3BitsPerSlot     = 64 // uint64
	v3SlotsPerSlotset = 8  // 每个 slotset 含 8 个 slot，共 512 bits

	// L1 entry 编码（每 2 bit）
	v3L1EntryWidth     = 2
	v3L1EntryMask      = (1 << v3L1EntryWidth) - 1      // 0x3
	v3L1EntryFull      = 0x0                            // 全部已分配
	v3L1EntryPartial   = 0x1                            // 部分空闲
	v3L1EntryFree      = 0x3                            // 全部空闲
	v3L1EntriesPerSlot = v3BitsPerSlot / v3L1EntryWidth // 32
)

// bitsPerSlotset = 8 * 64 = 512（一个 L0 slotset 覆盖的 block 数，对应 1 条 L1 entry）
const v3BitsPerSlotset = v3SlotsPerSlotset * v3BitsPerSlot

// v3BitsPerL1Slotset = 8 * 32 * 512 = 131072（一个 L1 slotset 覆盖的 block 数，对应 1 条 L2 entry）
const v3BitsPerL1Slotset = v3SlotsPerSlotset * v3L1EntriesPerSlot * v3BitsPerSlotset

// AllocatorV3 是参考 Ceph fastbmap 设计的三层 bitmap 分配器。
// 优先分配连续空间，通过 L2->L1->L0 三层跳过已满区域，减少扫描开销。
type AllocatorV3 struct {
	mu        sync.Mutex
	total     int64    // 总 block 数
	available int64    // 当前空闲 block 数
	l0        []uint64 // 细粒度 bitmap：1=空闲
	l1        []uint64 // L1 摘要：每 2bit 描述一个 slotset
	l2        []uint64 // L2 摘要：每 2bit 描述 L1 中一个 slotset
	l0Slots   int64    // l0 的 slot 数（uint64 数量）
	lastPos   int64    // 上次分配结束位置（block 偏移），用于顺序优先
}

// NewAllocatorV3 创建一个管理 blocks 个块的分配器，初始全部空闲。
// 例如：alloc := NewAllocatorV3(1024 * 1024) // 1M blocks
func NewAllocatorV3(blocks int64) *AllocatorV3 {
	a := &AllocatorV3{total: blocks}
	if blocks <= 0 {
		return a
	}

	// ── L0：精确分配，只覆盖 blocks 个 bit，不做大对齐扩容 ──────────────────
	// total 之外不存在任何 free bit，避免污染 L1/L2 摘要。
	l0SlotCount := (blocks + v3BitsPerSlot - 1) / v3BitsPerSlot
	a.l0Slots = l0SlotCount
	a.l0 = make([]uint64, l0SlotCount)
	for i := range a.l0 {
		a.l0[i] = ^uint64(0)
	}
	// 清除最后一个 word 中超出 total 的多余 bit
	if rem := uint(blocks % v3BitsPerSlot); rem != 0 {
		a.l0[l0SlotCount-1] = (uint64(1) << rem) - 1
	}

	// ── L1：按实际 L0 slotset 数量定大小，初始化为全 0（FULL）────────────────
	// 超出实际 L0 slotset 的 padding entry 保持 FULL(0)，
	// 不会被误判为可分配区域。
	l0Slotsets := (l0SlotCount + v3SlotsPerSlotset - 1) / v3SlotsPerSlotset
	l1SlotCount := (int64(l0Slotsets) + int64(v3L1EntriesPerSlot) - 1) / int64(v3L1EntriesPerSlot)
	if l1SlotCount < 1 {
		l1SlotCount = 1
	}
	a.l1 = make([]uint64, l1SlotCount) // zero value = all FULL (0x0)

	// ── L2：同理，初始化为全 0（FULL）────────────────────────────────────────
	l1Slotsets := (int64(len(a.l1)) + int64(v3SlotsPerSlotset) - 1) / int64(v3SlotsPerSlotset)
	l2SlotCount := (l1Slotsets + int64(v3L1EntriesPerSlot) - 1) / int64(v3L1EntriesPerSlot)
	if l2SlotCount < 1 {
		l2SlotCount = 1
	}
	a.l2 = make([]uint64, l2SlotCount) // zero value = all FULL (0x0)

	// ── 从 L0 自底向上重建 L1 和 L2 摘要 ────────────────────────────────────
	// 只遍历真实存在的 L0 slot；L1/L2 中超出范围的 entry 保持 FULL(0)，符合语义。
	for i := 0; i < int(l0SlotCount); i++ {
		a.updateL1ForL0Slot(i)
	}
	for li := 0; li < len(a.l1); li++ {
		a.updateL2ForL1Slot(li)
	}

	a.available = blocks
	return a
}

// Allocate 分配 want 个连续或碎片 block，优先从上次分配位置顺序搜索，
// 尽量返回连续大 extent。返回 []Result 表示分配到的 [offset, offset+size) 区间列表。
func (a *AllocatorV3) Allocate(want int64) ([]Result, error) {
	if a == nil {
		return nil, fmt.Errorf("allocator is nil")
	}
	if want <= 0 {
		return nil, fmt.Errorf("invalid allocate size=%d", want)
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	if a.available < want {
		return nil, fmt.Errorf("not enough space: want=%d available=%d", want, a.available)
	}

	results := make([]Result, 0, 4)
	left := want

	// 两遍扫描：先从 lastPos 开始，若不够则从头补齐
	for pass := 0; pass < 2 && left > 0; pass++ {
		var from int64
		if pass == 0 {
			from = a.lastPos
		} else {
			from = 0
		}
		for left > 0 {
			// 传入 left 作为 limit，最多扩展 left 个 block，避免过度扫描
			offset, size := a.nextFreeRunFromLocked(from, left)
			if size == 0 {
				break
			}
			// 避免第二遍重复扫描已经在第一遍扫过的区域
			if pass == 1 && offset >= a.lastPos {
				break
			}
			take := minInt64(size, left)
			a.markRangeLocked(offset, take, false)
			appendResult(&results, Result{Offset: offset, Size: take})
			left -= take
			from = offset + take
		}
	}

	if left > 0 {
		// 回滚
		for _, r := range results {
			a.markRangeLocked(r.Offset, r.Size, true)
		}
		return nil, fmt.Errorf("bitmap inconsistent: want=%d left=%d available=%d", want, left, a.available)
	}

	a.available -= want
	if len(results) > 0 {
		last := results[len(results)-1]
		a.lastPos = last.Offset + last.Size
		if a.lastPos >= a.total {
			a.lastPos = 0
		}
	}
	return results, nil
}

// Release 释放之前 Allocate 返回的 extents。重复释放返回错误。
func (a *AllocatorV3) Release(results []Result) error {
	if a == nil {
		return fmt.Errorf("allocator is nil")
	}
	if len(results) == 0 {
		return nil
	}

	a.mu.Lock()
	defer a.mu.Unlock()
	//生产可以注释，不需要
	for _, r := range results {
		if err := a.validateRangeV3(r.Offset, r.Size, "release"); err != nil {
			return err
		}
		if !a.rangeFullyAllocatedLocked(r.Offset, r.Size) {
			return fmt.Errorf("release extent not fully allocated: offset=%d size=%d", r.Offset, r.Size)
		}
	}
	// 检查 extents 间重叠, 生产可以注释，不需要
	for i := 0; i < len(results); i++ {
		for j := i + 1; j < len(results); j++ {
			if extentsOverlap(results[i], results[j]) {
				return fmt.Errorf("release extents overlap: %+v %+v", results[i], results[j])
			}
		}
	}

	var released int64
	for _, r := range results {
		released += a.markRangeLocked(r.Offset, r.Size, true)
	}
	a.available += released
	return nil
}

// InitAllocated 将 [offset, offset+size) 标记为已分配（用于从持久化状态恢复）。
func (a *AllocatorV3) InitAllocated(offset, size int64) error {
	if a == nil {
		return fmt.Errorf("allocator is nil")
	}
	if size <= 0 {
		return nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if err := a.validateRangeV3(offset, size, "InitAllocated"); err != nil {
		return err
	}
	changed := a.markRangeLocked(offset, size, false)
	a.available -= changed
	if a.available < 0 {
		return fmt.Errorf("available below zero after InitAllocated")
	}
	return nil
}

// InitFree 将 [offset, offset+size) 标记为空闲（用于从持久化状态恢复）。
func (a *AllocatorV3) InitFree(offset, size int64) error {
	if a == nil {
		return fmt.Errorf("allocator is nil")
	}
	if size <= 0 {
		return nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if err := a.validateRangeV3(offset, size, "InitFree"); err != nil {
		return err
	}
	changed := a.markRangeLocked(offset, size, true)
	a.available += changed
	if a.available > a.total {
		return fmt.Errorf("available exceeds total after InitFree")
	}
	return nil
}

// Available 返回当前空闲 block 数。
func (a *AllocatorV3) Available() int64 {
	if a == nil {
		return 0
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.available
}

// Total 返回总 block 数。
func (a *AllocatorV3) Total() int64 {
	if a == nil {
		return 0
	}
	return a.total
}

// FreeBlocks 是 Available 的别名，兼容旧接口。
func (a *AllocatorV3) FreeBlocks() int64 {
	return a.Available()
}

// ---- 内部方法 ----

func (a *AllocatorV3) validateRangeV3(offset, size int64, op string) error {
	end := offset + size
	if offset < 0 || size <= 0 || end < offset || end > a.total {
		return fmt.Errorf("invalid %s extent offset=%d size=%d total=%d", op, offset, size, a.total)
	}
	return nil
}

// nextFreeRunFromLocked 从 from（block 偏移）开始，经由 L2→L1→L0 三层搜索，
// 找到下一段连续空闲区间并返回其起始 block 和长度。
// limit 指本次最多需要多少 block，摘要层利用此值在 FREE 时直接截断，避免无效扩展。
//
// 层次结构（各层覆盖粒度）：
//
//	L0 slot     : 1 uint64 = 64 blocks
//	L0 slotset  : 8 L0 slot = 512 blocks       → 对应 1 条 L1 entry（2-bit）
//	L1 slot     : 1 uint64 = 32 L1 entry = 16384 blocks
//	L1 slotset  : 8 L1 slot = 131072 blocks    → 对应 1 条 L2 entry（2-bit）
//	L2 slot     : 1 uint64 = 32 L2 entry = 4194304 blocks
//
// 每层 entry 处理规则：
//
//	FULL    → 跳过（整段已满）
//	FREE    → 直接返回覆盖范围内的一段，size = min(limit, 剩余有效 blocks）
//	PARTIAL → 下钻到下一层精确查找
func (a *AllocatorV3) nextFreeRunFromLocked(from, limit int64) (offset, size int64) {
	if from >= a.total || len(a.l0) == 0 {
		return 0, 0
	}

	// 将 block 偏移映射到各层起始索引
	l0SlotIdx := int(from / v3BitsPerSlot)
	l0SetIdx := l0SlotIdx / v3SlotsPerSlotset  // L0 slotset index（= 需找的 L1 entry 序号）
	l1SlotIdx := l0SetIdx / v3L1EntriesPerSlot // L1 slot index
	l1SetIdx := l1SlotIdx / v3SlotsPerSlotset  // L1 slotset index（= 需找的 L2 entry 序号）
	l2SlotIdx := l1SetIdx / v3L1EntriesPerSlot // L2 slot index
	initL2Bit := l1SetIdx % v3L1EntriesPerSlot // 在 L2 slot 内的起始 entry 位置

	// ── L2 层：每个 entry 代表 131072 blocks ─────────────────────────────────
	for curL2Slot := l2SlotIdx; curL2Slot < len(a.l2); curL2Slot++ {
		l2Word := a.l2[curL2Slot]
		startL2Bit := 0
		if curL2Slot == l2SlotIdx {
			startL2Bit = initL2Bit
		}

		for curL2Bit := startL2Bit; curL2Bit < v3L1EntriesPerSlot; curL2Bit++ {
			l2Entry := (l2Word >> uint(curL2Bit*v3L1EntryWidth)) & v3L1EntryMask

			if l2Entry == v3L1EntryFull {
				continue // L2_FULL：整个 L1 slotset 已满，跳过
			}

			// 计算本 L2 entry 覆盖的 block 范围
			l1SetGlobal := curL2Slot*v3L1EntriesPerSlot + curL2Bit
			l2EntryStart := int64(l1SetGlobal) * v3BitsPerL1Slotset
			l2EntryEnd := l2EntryStart + v3BitsPerL1Slotset
			if l2EntryEnd > a.total {
				l2EntryEnd = a.total
			}

			if l2Entry == v3L1EntryFree {
				// L2_FREE 快路径：整个 L1 slotset 全空闲，无需下钻 L1/L0
				actualStart := l2EntryStart
				if from > actualStart {
					actualStart = from
				}
				sz := minInt64(limit, l2EntryEnd-actualStart)
				if sz > 0 {
					return actualStart, sz
				}
				continue
			}

			// L2_PARTIAL：进入 L1 层 ──────────────────────────────────────────
			l1SlotStart := l1SetGlobal * v3SlotsPerSlotset
			l1SlotEnd := l1SlotStart + v3SlotsPerSlotset
			if l1SlotEnd > len(a.l1) {
				l1SlotEnd = len(a.l1)
			}

			startL1Slot := l1SlotStart
			initL1Bit := 0
			if curL2Slot == l2SlotIdx && curL2Bit == initL2Bit {
				startL1Slot = l1SlotIdx
				initL1Bit = l0SetIdx % v3L1EntriesPerSlot
			}

			for li := startL1Slot; li < l1SlotEnd; li++ {
				l1Word := a.l1[li]
				startL1Bit := 0
				if li == startL1Slot {
					startL1Bit = initL1Bit
				}

				for bitPos := startL1Bit; bitPos < v3L1EntriesPerSlot; bitPos++ {
					l1Entry := (l1Word >> uint(bitPos*v3L1EntryWidth)) & v3L1EntryMask

					if l1Entry == v3L1EntryFull {
						continue // L1_FULL：整个 L0 slotset 已满，跳过
					}

					// 计算本 L1 entry 覆盖的 block 范围（一个 L0 slotset）
					l0SetGlobal := li*v3L1EntriesPerSlot + bitPos
					l0SlotStart := int64(l0SetGlobal) * v3SlotsPerSlotset
					slotStartBlock := l0SlotStart * v3BitsPerSlot
					slotEndBlock := slotStartBlock + v3BitsPerSlotset
					if slotEndBlock > a.total {
						slotEndBlock = a.total
					}

					if l1Entry == v3L1EntryFree {
						// L1_FREE 快路径：整个 L0 slotset 全空闲，无需下钻 L0
						actualStart := slotStartBlock
						if from > actualStart {
							actualStart = from
						}
						sz := minInt64(limit, slotEndBlock-actualStart)
						if sz > 0 {
							return actualStart, sz
						}
						continue
					}

					// L1_PARTIAL：进入 L0 层精确扫描 ──────────────────────────
					l0SlotEnd := l0SlotStart + v3SlotsPerSlotset
					if l0SlotEnd > int64(len(a.l0)) {
						l0SlotEnd = int64(len(a.l0))
					}
					bFrom := from
					if slotStartBlock > from {
						bFrom = slotStartBlock
					}
					off, sz := a.scanL0ForFreeRunLimited(bFrom, l0SlotEnd*v3BitsPerSlot, limit)
					if sz > 0 {
						return off, sz
					}
				}
			}
		}
	}
	return 0, 0
}

// scanL0ForFullFreeRun 在 [fromBit, toBit) 内寻找第一个空闲 bit，
// 找到后无限制地向右扩展连续空闲 run（可越过 toBit），
// 适合需要获取最大连续 extent 的场景（如统计、预分配）。
func (a *AllocatorV3) scanL0ForFullFreeRun(fromBit, toBit int64) (offset, size int64) {
	return a.scanL0ForFreeRunLimited(fromBit, toBit, a.total)
}

// scanL0ForFreeRunLimited 在 [fromBit, toBit) 内寻找第一个空闲 bit，
// 找到后最多向右扩展 limit 个 block 后立即返回。
// 用于分配路径：调用方传入本次还需要多少（left），避免扫描无用的已分配区域。
func (a *AllocatorV3) scanL0ForFreeRunLimited(fromBit, toBit, limit int64) (offset, size int64) {
	if fromBit >= toBit || fromBit >= a.total || limit <= 0 {
		return 0, 0
	}
	if toBit > a.total {
		toBit = a.total
	}

	startWord := int(fromBit / v3BitsPerSlot)
	bitOff := uint(fromBit % v3BitsPerSlot)

	// ── 第一步：在 [fromBit, toBit) 内找第一个空闲 bit ──────────────────────
	word := a.l0[startWord] >> bitOff
	var firstBit int64 = -1
	if word != 0 {
		tz := bits.TrailingZeros64(word)
		firstBit = int64(startWord)*v3BitsPerSlot + int64(bitOff) + int64(tz)
	} else {
		endWord := int((toBit - 1) / v3BitsPerSlot)
		for w := startWord + 1; w <= endWord; w++ {
			if a.l0[w] != 0 {
				tz := bits.TrailingZeros64(a.l0[w])
				firstBit = int64(w)*v3BitsPerSlot + int64(tz)
				break
			}
		}
	}

	if firstBit < 0 || firstBit >= toBit || firstBit >= a.total {
		return 0, 0
	}

	// ── 第二步：从 firstBit 向右扩展连续空闲 run，最多 limit 个 block ────────
	runStart := firstBit
	runEnd := runStart
	cap := limit // 剩余可扩展配额

	w := int(firstBit / v3BitsPerSlot)
	b := uint(firstBit % v3BitsPerSlot)
	for w < len(a.l0) && cap > 0 {
		shifted := a.l0[w] >> b
		n := bits.TrailingZeros64(^shifted) // 该 word 从 b 位起连续空闲的个数
		avail := v3BitsPerSlot - int(b)     // 该 word 从 b 位起的剩余 bit 数
		if n > avail {
			n = avail
		}
		// 不超过剩余配额
		if int64(n) > cap {
			n = int(cap)
		}
		runEnd += int64(n)
		cap -= int64(n)
		// 遇到已分配 bit 或达到 limit，停止扩展
		if n < avail || cap == 0 {
			break
		}
		w++
		b = 0
	}

	if runEnd > a.total {
		runEnd = a.total
	}

	sz := runEnd - runStart
	if sz <= 0 {
		return 0, 0
	}
	return runStart, sz
}

// markRangeLocked 将 [offset, offset+size) 的 bits 设置为 free(true) 或 allocated(false)。
// 返回实际改变的 bit 数。同步更新 L1 和 L2。
//
// 优化：先批量改完所有 L0 word，再按 slotset 边界去重更新 L1；
// 最后按 L1 slotset 去重更新 L2。避免大范围写入时对同一 L1/L2 entry 的重复刷新。
func (a *AllocatorV3) markRangeLocked(offset, size int64, free bool) int64 {
	if size <= 0 {
		return 0
	}
	end := offset + size

	startWord := int(offset / v3BitsPerSlot)
	endWord := int((end - 1) / v3BitsPerSlot)

	// ── 第一步：批量更新 L0，仅累计变化量，不触碰 L1 ─────────────────────────
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
	}

	// ── 第二步：按 slotset 去重更新 L1 ──────────────────────────────────────
	// 一个 L0 slotset = v3SlotsPerSlotset(8) 个 word；同一 slotset 只调用一次。
	startSlotset := startWord / v3SlotsPerSlotset
	endSlotset := endWord / v3SlotsPerSlotset
	for ss := startSlotset; ss <= endSlotset; ss++ {
		// updateL1ForL0Slot 接受任意属于该 slotset 的 word 下标即可
		a.updateL1ForL0Slot(ss * v3SlotsPerSlotset)
	}

	// ── 第三步：按 L1 slotset 去重更新 L2 ───────────────────────────────────
	// startL1Slot := startSlotset / v3L1EntriesPerSlot
	// endL1Slot := endSlotset / v3L1EntriesPerSlot
	// for li := startL1Slot; li <= endL1Slot && li < len(a.l1); li++ {
	// 	a.updateL2ForL1Slot(li)
	// }

	// ── 第三步：按 L1 slotset 去重更新 L2 ───────────────────────────────────
	// 1 个 L1 slotset = v3SlotsPerSlotset(8) 个 L1 slot。
	// 修复：折叠为 L1 slotset 范围，每个 L1 slotset 只调用一次。
	startL1Slot := startSlotset / v3L1EntriesPerSlot
	endL1Slot := endSlotset / v3L1EntriesPerSlot
	startL1Slotset := startL1Slot / v3SlotsPerSlotset
	endL1Slotset := endL1Slot / v3SlotsPerSlotset
	for l1Set := startL1Slotset; l1Set <= endL1Slotset; l1Set++ {
		li := l1Set * v3SlotsPerSlotset // 该 L1 slotset 内首个 L1 slot 下标
		if li >= len(a.l1) {
			break
		}
		a.updateL2ForL1Slot(li)
	}

	return changed
}

// updateL1ForL0Slot 根据 L0 slot 的状态更新对应的 L1 entry（2bit）。
func (a *AllocatorV3) updateL1ForL0Slot(l0SlotIdx int) {
	slotsetIdx := l0SlotIdx / v3SlotsPerSlotset
	l1SlotIdx := slotsetIdx / v3L1EntriesPerSlot
	l1BitPos := slotsetIdx % v3L1EntriesPerSlot
	if l1SlotIdx >= len(a.l1) {
		return
	}

	// 检查该 slotset 所有 L0 slot 的状态
	ssStart := slotsetIdx * v3SlotsPerSlotset
	ssEnd := ssStart + v3SlotsPerSlotset
	if ssEnd > len(a.l0) {
		ssEnd = len(a.l0)
	}

	allFree := true
	allAlloc := true
	for k := ssStart; k < ssEnd; k++ {
		mask := a.v3ValidWordMask(k)
		if a.l0[k]&mask != mask {
			allFree = false
		}
		if a.l0[k]&mask != 0 {
			allAlloc = false
		}
	}

	var entry uint64
	if allFree {
		entry = v3L1EntryFree
	} else if allAlloc {
		entry = v3L1EntryFull
	} else {
		entry = v3L1EntryPartial
	}

	shift := uint(l1BitPos * v3L1EntryWidth)
	a.l1[l1SlotIdx] = (a.l1[l1SlotIdx] &^ (uint64(v3L1EntryMask) << shift)) | (entry << shift)
}

// updateL2ForL1Slot 根据 L1 slot 的状态更新对应的 L2 entry（2bit）。
func (a *AllocatorV3) updateL2ForL1Slot(l1SlotIdx int) {
	if len(a.l2) == 0 {
		return
	}
	l1SlotsetIdx := l1SlotIdx / v3SlotsPerSlotset
	l2SlotIdx := l1SlotsetIdx / v3L1EntriesPerSlot
	l2BitPos := l1SlotsetIdx % v3L1EntriesPerSlot
	if l2SlotIdx >= len(a.l2) {
		return
	}

	ssStart := l1SlotsetIdx * v3SlotsPerSlotset
	ssEnd := ssStart + v3SlotsPerSlotset
	if ssEnd > len(a.l1) {
		ssEnd = len(a.l1)
	}

	allFree := true
	allAlloc := true
	for k := ssStart; k < ssEnd; k++ {
		if a.l1[k] != ^uint64(0) {
			allFree = false
		}
		if a.l1[k] != 0 {
			allAlloc = false
		}
	}

	var entry uint64
	if allFree {
		entry = v3L1EntryFree
	} else if allAlloc {
		entry = v3L1EntryFull
	} else {
		entry = v3L1EntryPartial
	}

	shift := uint(l2BitPos * v3L1EntryWidth)
	a.l2[l2SlotIdx] = (a.l2[l2SlotIdx] &^ (uint64(v3L1EntryMask) << shift)) | (entry << shift)
}

// rangeFullyAllocatedLocked 检查 [offset, offset+size) 是否全部已分配（bits 全为 0）。
func (a *AllocatorV3) rangeFullyAllocatedLocked(offset, size int64) bool {
	end := offset + size
	startWord := int(offset / v3BitsPerSlot)
	endWord := int((end - 1) / v3BitsPerSlot)
	for i := startWord; i <= endWord && i < len(a.l0); i++ {
		mask := v3RangeMask(offset, end, i) & a.v3ValidWordMask(i)
		if a.l0[i]&mask != 0 {
			return false
		}
	}
	return true
}

// v3ValidWordMask 返回 l0 第 index 个 slot 的有效 bit 掩码（最后一个 slot 可能不满 64 bit）。
func (a *AllocatorV3) v3ValidWordMask(index int) uint64 {
	if index < len(a.l0)-1 {
		return ^uint64(0)
	}
	rem := uint(a.total % v3BitsPerSlot)
	if rem == 0 {
		return ^uint64(0)
	}
	return (uint64(1) << rem) - 1
}

// v3RangeMask 生成覆盖 [offset, end) 在第 wordIndex 个 uint64 中的 bit 掩码。
func v3RangeMask(offset, end int64, wordIndex int) uint64 {
	wordStart := int64(wordIndex * v3BitsPerSlot)
	wordEnd := wordStart + v3BitsPerSlot
	start := maxInt64(offset, wordStart)
	e := minInt64(end, wordEnd)
	width := uint(e - start)
	shift := uint(start - wordStart)
	if width == 0 {
		return 0
	}
	if width >= v3BitsPerSlot {
		return ^uint64(0)
	}
	return ((uint64(1) << width) - 1) << shift
}

// alignUp 将 v 向上对齐到 align 的倍数。
func alignUp(v, align int64) int64 {
	if align <= 0 {
		return v
	}
	return (v + align - 1) / align * align
}
