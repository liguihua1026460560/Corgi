package fs

import (
	"sync"
	"sync/atomic"
)

type alignedBufferPool struct {
	mu    sync.RWMutex
	pools map[int]*sync.Pool

	gets atomic.Int64
	news atomic.Int64
	puts atomic.Int64
}

var defaultAlignedBufferPool = &alignedBufferPool{
	pools: make(map[int]*sync.Pool),
}

func GetAlignedBlockBuffer(size int) []byte {
	return defaultAlignedBufferPool.Get(size)
}

func PutAlignedBlockBuffer(buf []byte) {
	defaultAlignedBufferPool.Put(buf)
}

func (p *alignedBufferPool) Get(size int) []byte {
	if size <= 0 {
		return nil
	}

	p.gets.Add(1)

	pool := p.getPool(size)
	if v := pool.Get(); v != nil {
		buf := v.([]byte)
		return buf[:size]
	}

	p.news.Add(1)
	return NewAlignedBlockBuffer(size)
}

func (p *alignedBufferPool) Put(buf []byte) {
	if len(buf) == 0 {
		return
	}

	p.puts.Add(1)

	size := len(buf)
	pool := p.getPool(size)
	pool.Put(buf[:size])
}

func (p *alignedBufferPool) getPool(size int) *sync.Pool {
	p.mu.RLock()
	pool := p.pools[size]
	p.mu.RUnlock()
	if pool != nil {
		return pool
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if pool = p.pools[size]; pool != nil {
		return pool
	}

	pool = &sync.Pool{}
	p.pools[size] = pool
	return pool
}

type AlignedBufferPoolStats struct {
	Gets int64
	News int64
	Puts int64
}

func AlignedBufferPoolSnapshot() AlignedBufferPoolStats {
	return AlignedBufferPoolStats{
		Gets: defaultAlignedBufferPool.gets.Load(),
		News: defaultAlignedBufferPool.news.Load(),
		Puts: defaultAlignedBufferPool.puts.Load(),
	}
}
