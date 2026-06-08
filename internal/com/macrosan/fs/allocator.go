package fs

import (
	"fmt"
	"runtime"
	"sort"
	"sync"
)

type allocatorShard struct {
	start     int64
	size      int64
	mu        sync.Mutex
	allocator *AllocatorV3
}

type ShardedAllocator struct {
	shards      []allocatorShard
	totalBlocks int64
}

func NewShardedAllocator(blocks int64) *ShardedAllocator {
	n := runtime.NumCPU()
	if n > 32 {
		n = 32
	}
	if n < 1 {
		n = 1
	}
	if blocks > 0 && int64(n) > blocks {
		n = int(blocks)
	}

	a := &ShardedAllocator{
		shards:      make([]allocatorShard, n),
		totalBlocks: blocks,
	}

	base := int64(0)
	if n > 0 {
		base = blocks / int64(n)
	}
	rem := int64(0)
	if n > 0 {
		rem = blocks % int64(n)
	}

	start := int64(0)
	for i := range a.shards {
		size := base
		if int64(i) < rem {
			size++
		}
		a.shards[i] = allocatorShard{
			start:     start,
			size:      size,
			allocator: NewAllocatorV3(size),
		}
		start += size
	}
	return a
}

func (a *ShardedAllocator) Allocate(want int64) []Result {
	if a == nil || want <= 0 || len(a.shards) == 0 {
		return nil
	}

	start := 0
	for i := 0; i < len(a.shards); i++ {
		idx := (start + i) % len(a.shards)
		shard := &a.shards[idx]
		shard.mu.Lock()
		results, err := shard.allocator.Allocate(want)
		shard.mu.Unlock()
		if err == nil && len(results) > 0 {
			addResultOffset(results, shard.start)
			return results
		}
	}

	left := want
	results := make([]Result, 0, len(a.shards))
	for i := 0; i < len(a.shards) && left > 0; i++ {
		idx := (start + i) % len(a.shards)
		shard := &a.shards[idx]

		shard.mu.Lock()
		take := minInt64(left, shard.allocator.FreeBlocks())
		if take > 0 {
			part, err := shard.allocator.Allocate(take)
			if err != nil {
				shard.mu.Unlock()
				a.Release(results)
				return nil
			}
			addResultOffset(part, shard.start)
			for _, r := range part {
				appendResult(&results, r)
			}
			left -= resultBlocks(part)
		}
		shard.mu.Unlock()
	}

	if left == 0 {
		return results
	}
	a.Release(results)
	return nil
}

func (a *ShardedAllocator) Release(results []Result) {
	_ = a.ReleaseErr(results)
}

func (a *ShardedAllocator) ReleaseErr(results []Result) error {
	if a == nil || len(results) == 0 {
		return nil
	}
	for _, r := range results {
		if err := a.releaseOne(r); err != nil {
			return err
		}
	}
	return nil
}

func (a *ShardedAllocator) InitAllocated(offset, size int64) error {
	if a == nil || size <= 0 {
		return nil
	}
	end := offset + size
	if offset < 0 || end < offset || end > a.totalBlocks {
		return fmt.Errorf("invalid allocated extent offset=%d size=%d", offset, size)
	}

	cur := offset
	for cur < end {
		idx := a.shardIndex(cur)
		if idx < 0 {
			return fmt.Errorf("allocated extent offset=%d outside allocator", cur)
		}
		shard := &a.shards[idx]
		next := minInt64(end, shard.start+shard.size)
		shard.mu.Lock()
		err := shard.allocator.InitAllocated(cur-shard.start, next-cur)
		shard.mu.Unlock()
		if err != nil {
			return err
		}
		cur = next
	}
	return nil
}

func (a *ShardedAllocator) InitFree(offset, size int64) error {
	if a == nil || size <= 0 {
		return nil
	}
	end := offset + size
	if offset < 0 || end < offset || end > a.totalBlocks {
		return fmt.Errorf("invalid free extent offset=%d size=%d", offset, size)
	}

	cur := offset
	for cur < end {
		idx := a.shardIndex(cur)
		if idx < 0 {
			return fmt.Errorf("free extent offset=%d outside allocator", cur)
		}
		shard := &a.shards[idx]
		next := minInt64(end, shard.start+shard.size)
		shard.mu.Lock()
		err := shard.allocator.InitFree(cur-shard.start, next-cur)
		shard.mu.Unlock()
		if err != nil {
			return err
		}
		cur = next
	}
	return nil
}

func (a *ShardedAllocator) FreeBlocks() int64 {
	if a == nil {
		return 0
	}
	var total int64
	for i := range a.shards {
		shard := &a.shards[i]
		shard.mu.Lock()
		total += shard.allocator.FreeBlocks()
		shard.mu.Unlock()
	}
	return total
}

func (a *ShardedAllocator) Available() int64 {
	return a.FreeBlocks()
}

func (a *ShardedAllocator) Total() int64 {
	if a == nil {
		return 0
	}
	return a.totalBlocks
}

func (a *ShardedAllocator) releaseOne(r Result) error {
	if r.Size <= 0 {
		return fmt.Errorf("invalid release extent offset=%d size=%d", r.Offset, r.Size)
	}

	end := r.Offset + r.Size
	if r.Offset < 0 || end < r.Offset || end > a.totalBlocks {
		return fmt.Errorf("invalid release extent offset=%d size=%d", r.Offset, r.Size)
	}

	cur := r.Offset
	for cur < end {
		idx := a.shardIndex(cur)
		if idx < 0 {
			return fmt.Errorf("release extent offset=%d outside allocator", cur)
		}
		shard := &a.shards[idx]
		next := minInt64(end, shard.start+shard.size)
		shard.mu.Lock()
		err := shard.allocator.Release([]Result{{
			Offset: cur - shard.start,
			Size:   next - cur,
		}})
		shard.mu.Unlock()
		if err != nil {
			return err
		}
		cur = next
	}
	return nil
}

func (a *ShardedAllocator) shardIndex(offset int64) int {
	if offset < 0 || offset >= a.totalBlocks {
		return -1
	}
	return sort.Search(len(a.shards), func(i int) bool {
		return a.shards[i].start+a.shards[i].size > offset
	})
}

func addResultOffset(results []Result, offset int64) {
	for i := range results {
		results[i].Offset += offset
	}
}

func resultBlocks(results []Result) int64 {
	var total int64
	for _, r := range results {
		total += r.Size
	}
	return total
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
