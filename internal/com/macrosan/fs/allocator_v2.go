package fs

import (
	"fmt"
	"math/bits"
	"sync"
)

const allocatorBitsPerWord = 64

// Allocator is a bitmap based in-memory allocator.
//
// A set bit in l0 means the corresponding block is free. l1 is a summary
// bitmap where each set bit means the corresponding l0 word has at least one
// free block. This keeps the common sequential scan cheap while still keeping
// the allocator small and Go-native.
type AllocatorV2 struct {
	mu        sync.Mutex
	total     int64
	available int64
	l0        []uint64
	l1        []uint64
}

func NewAllocatorV2(blocks int64) *AllocatorV2 {
	a := &AllocatorV2{total: blocks}
	if blocks <= 0 {
		a.total = 0
		return a
	}

	wordCount := wordsForBits(blocks)
	a.l0 = make([]uint64, wordCount)
	for i := range a.l0 {
		a.l0[i] = ^uint64(0)
	}
	a.l0[len(a.l0)-1] &= a.validWordMask(len(a.l0) - 1)

	a.l1 = make([]uint64, wordsForBits(int64(wordCount)))
	for i, word := range a.l0 {
		if word != 0 {
			a.setL1Bit(i)
		}
	}
	a.available = blocks
	return a
}

func (a *AllocatorV2) Allocate(want int64) ([]Result, error) {
	if a == nil {
		return nil, fmt.Errorf("allocator is nil")
	}
	if want <= 0 {
		return nil, fmt.Errorf("invalid allocate size=%d", want)
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	if a.available < want {
		return nil, fmt.Errorf("allocator has no enough space: want=%d available=%d", want, a.available)
	}

	left := want
	pos := int64(0)
	results := make([]Result, 0, 1)
	for left > 0 {
		offset, size := a.nextFreeRunLocked(pos)
		if size == 0 {
			return nil, fmt.Errorf("allocator bitmap inconsistent: want=%d left=%d available=%d", want, left, a.available)
		}

		take := minInt64(size, left)
		a.setRangeLocked(offset, take, false)
		appendResult(&results, Result{Offset: offset, Size: take})

		left -= take
		pos = offset + take
	}
	a.available -= want
	return results, nil
}

func (a *AllocatorV2) Release(results []Result) error {
	if a == nil {
		return fmt.Errorf("allocator is nil")
	}
	if len(results) == 0 {
		return nil
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	for _, r := range results {
		if err := a.validateRange(r.Offset, r.Size, "release"); err != nil {
			return err
		}
		if !a.rangeAllocatedLocked(r.Offset, r.Size) {
			return fmt.Errorf("release extent is not fully allocated: offset=%d size=%d", r.Offset, r.Size)
		}
	}
	for i := 0; i < len(results); i++ {
		for j := i + 1; j < len(results); j++ {
			if extentsOverlap(results[i], results[j]) {
				return fmt.Errorf("release extents overlap: left=%+v right=%+v", results[i], results[j])
			}
		}
	}

	var released int64
	for _, r := range results {
		released += a.setRangeLocked(r.Offset, r.Size, true)
	}
	a.available += released
	if a.available > a.total {
		return fmt.Errorf("allocator available exceeds total: available=%d total=%d", a.available, a.total)
	}
	return nil
}

func (a *AllocatorV2) InitAllocated(offset, size int64) error {
	if a == nil {
		return fmt.Errorf("allocator is nil")
	}
	if size <= 0 {
		return nil
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	if err := a.validateRange(offset, size, "allocated"); err != nil {
		return err
	}
	changed := a.setRangeLocked(offset, size, false)
	a.available -= changed
	if a.available < 0 {
		return fmt.Errorf("allocator available below zero: available=%d", a.available)
	}
	return nil
}

func (a *AllocatorV2) InitFree(offset, size int64) error {
	if a == nil {
		return fmt.Errorf("allocator is nil")
	}
	if size <= 0 {
		return nil
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	if err := a.validateRange(offset, size, "free"); err != nil {
		return err
	}
	changed := a.setRangeLocked(offset, size, true)
	a.available += changed
	if a.available > a.total {
		return fmt.Errorf("allocator available exceeds total: available=%d total=%d", a.available, a.total)
	}
	return nil
}

func (a *AllocatorV2) Available() int64 {
	if a == nil {
		return 0
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.available
}

func (a *AllocatorV2) Total() int64 {
	if a == nil {
		return 0
	}
	return a.total
}

func (a *AllocatorV2) FreeBlocks() int64 {
	return a.Available()
}

func (a *AllocatorV2) validateRange(offset, size int64, op string) error {
	end := offset + size
	if offset < 0 || size <= 0 || end < offset || end > a.total {
		return fmt.Errorf("invalid %s extent offset=%d size=%d total=%d", op, offset, size, a.total)
	}
	return nil
}

func (a *AllocatorV2) nextFreeRunLocked(from int64) (int64, int64) {
	if from >= a.total {
		return 0, 0
	}

	wordIndex := int(from / allocatorBitsPerWord)
	bitOffset := uint(from % allocatorBitsPerWord)
	word := a.l0[wordIndex] & (^uint64(0) << bitOffset)
	if word == 0 {
		wordIndex = a.nextL0WordWithFreeLocked(wordIndex + 1)
		if wordIndex < 0 {
			return 0, 0
		}
		word = a.l0[wordIndex]
	}

	startBit := bits.TrailingZeros64(word)
	start := int64(wordIndex*allocatorBitsPerWord + startBit)
	if start >= a.total {
		return 0, 0
	}

	var run int64
	i := wordIndex
	bit := uint(startBit)
	for i < len(a.l0) {
		shifted := a.l0[i] >> bit
		n := bits.TrailingZeros64(^shifted)
		if n > allocatorBitsPerWord-int(bit) {
			n = allocatorBitsPerWord - int(bit)
		}
		run += int64(n)
		if n < allocatorBitsPerWord-int(bit) {
			break
		}
		i++
		bit = 0
	}

	if maxRun := a.total - start; run > maxRun {
		run = maxRun
	}
	return start, run
}

func (a *AllocatorV2) nextL0WordWithFreeLocked(from int) int {
	if from >= len(a.l0) {
		return -1
	}
	l1Index := from / allocatorBitsPerWord
	l1Bit := uint(from % allocatorBitsPerWord)
	word := a.l1[l1Index] & (^uint64(0) << l1Bit)
	for {
		if word != 0 {
			return l1Index*allocatorBitsPerWord + bits.TrailingZeros64(word)
		}
		l1Index++
		if l1Index >= len(a.l1) {
			return -1
		}
		word = a.l1[l1Index]
	}
}

func (a *AllocatorV2) rangeAllocatedLocked(offset, size int64) bool {
	startWord := int(offset / allocatorBitsPerWord)
	endWord := int((offset + size - 1) / allocatorBitsPerWord)
	for i := startWord; i <= endWord; i++ {
		mask := rangeMask(offset, size, i) & a.validWordMask(i)
		if a.l0[i]&mask != 0 {
			return false
		}
	}
	return true
}

func (a *AllocatorV2) setRangeLocked(offset, size int64, free bool) int64 {
	startWord := int(offset / allocatorBitsPerWord)
	endWord := int((offset + size - 1) / allocatorBitsPerWord)

	var changed int64
	for i := startWord; i <= endWord; i++ {
		mask := rangeMask(offset, size, i) & a.validWordMask(i)
		old := a.l0[i]
		var next uint64
		if free {
			changed += int64(bits.OnesCount64(^old & mask))
			next = old | mask
		} else {
			changed += int64(bits.OnesCount64(old & mask))
			next = old &^ mask
		}
		a.setL0WordLocked(i, next)
	}
	return changed
}

func (a *AllocatorV2) setL0WordLocked(index int, word uint64) {
	a.l0[index] = word & a.validWordMask(index)
	if a.l0[index] == 0 {
		a.clearL1Bit(index)
		return
	}
	a.setL1Bit(index)
}

func (a *AllocatorV2) setL1Bit(l0Index int) {
	a.l1[l0Index/allocatorBitsPerWord] |= uint64(1) << uint(l0Index%allocatorBitsPerWord)
}

func (a *AllocatorV2) clearL1Bit(l0Index int) {
	a.l1[l0Index/allocatorBitsPerWord] &^= uint64(1) << uint(l0Index%allocatorBitsPerWord)
}

func (a *AllocatorV2) validWordMask(index int) uint64 {
	if index < len(a.l0)-1 {
		return ^uint64(0)
	}
	rem := uint(a.total % allocatorBitsPerWord)
	if rem == 0 {
		return ^uint64(0)
	}
	return (uint64(1) << rem) - 1
}

func rangeMask(offset, size int64, wordIndex int) uint64 {
	wordStart := int64(wordIndex * allocatorBitsPerWord)
	wordEnd := wordStart + allocatorBitsPerWord
	start := maxInt64(offset, wordStart)
	end := minInt64(offset+size, wordEnd)
	width := uint(end - start)
	shift := uint(start - wordStart)
	if width == allocatorBitsPerWord {
		return ^uint64(0)
	}
	return ((uint64(1) << width) - 1) << shift
}

func wordsForBits(n int64) int {
	if n <= 0 {
		return 0
	}
	return int((n + allocatorBitsPerWord - 1) / allocatorBitsPerWord)
}

func appendResult(results *[]Result, r Result) {
	if r.Size <= 0 {
		return
	}
	n := len(*results)
	if n > 0 && (*results)[n-1].Offset+(*results)[n-1].Size == r.Offset {
		(*results)[n-1].Size += r.Size
		return
	}
	*results = append(*results, r)
}

func extentsOverlap(a, b Result) bool {
	return a.Offset < b.Offset+b.Size && b.Offset < a.Offset+a.Size
}
