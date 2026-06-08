package fs

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"sync"

	"github.com/cockroachdb/pebble/v2"
	"golang.org/x/sys/unix"
	mspebble "mossserver/internal/com/macrosan/database/pebble"
)

const (
	BlockSize    int64 = 4 * 1024
	MinAllocSize int64 = 1024 * 1024

	SpaceSize int64 = 1 << 24 // 16 MB
	SpaceLen        = int(SpaceSize / BlockSize / 8)
)

const blocksPerSpace = SpaceSize / BlockSize

type Result struct {
	Offset int64
	Size   int64
}

type BlockDevice struct {
	name     string
	path     string
	mountDir string
	size     int64

	file      *os.File
	db        *mspebble.MSPebble
	closeFunc func() error

	allocator *AllocatorV3
	Channel   *AioChannel

	bitmapMu sync.Mutex
}

// type Allocator interface {
// 	Allocate(want int64) ([]Result, error)
// 	Release(results []Result)
// 	InitAllocated(offset, size int64) error
// }

func OpenPartitionBlockDevice(ctx context.Context, lun, mountDir, dataPath string) (*BlockDevice, error) {
	size, err := blockDeviceSize(dataPath)
	if err != nil {
		return nil, err
	}
	if size <= 0 {
		return nil, fmt.Errorf("invalid block device size for %s: %d", dataPath, size)
	}
	//计算块设备有多少个4kb。例如块设备大小是16MB，那么就有4096个4kb块。size=4096*4096=16MB
	size = size / BlockSize * BlockSize
	if size == 0 {
		return nil, fmt.Errorf("block device %s is smaller than one block", dataPath)
	}

	db, err := mspebble.OpenDataDB(lun, mountDir)
	if err != nil {
		return nil, err
	}
	mspebble.AddDataBatch(lun, db.DB)
	closeDB := func() error {
		mspebble.RemoveBatch(lun)
		return db.Close()
	}

	f, err := unix.Open(dataPath, unix.O_RDWR|unix.O_DIRECT|unix.O_CLOEXEC, 0666)
	if err != nil {
		_ = closeDB()
		return nil, err
	}

	return newBlockDevice(ctx, lun, mountDir, dataPath, os.NewFile(uintptr(f), dataPath), size, db, closeDB)
}

func newBlockDevice(ctx context.Context, name, mountDir, path string, f *os.File, size int64, db *mspebble.MSPebble, closeFunc func() error) (*BlockDevice, error) {
	d := &BlockDevice{
		name:      name,
		path:      path,
		mountDir:  mountDir,
		size:      size,
		file:      f,
		db:        db,
		closeFunc: closeFunc,
		//allocator: NewShardedAllocator(size / BlockSize), //块设备有多少个块，就创建一个对应大小的分配器
		allocator: NewAllocatorV3(size / BlockSize),
	}

	if err := d.tryInitBlockSpace(ctx); err != nil {
		_ = f.Close()
		if closeFunc != nil {
			_ = closeFunc()
		}
		return nil, err
	}
	if err := d.loadAllocator(ctx); err != nil {
		_ = f.Close()
		if closeFunc != nil {
			_ = closeFunc()
		}
		return nil, err
	}

	channel, err := NewAioChannel(d)
	if err != nil {
		_ = f.Close()
		if closeFunc != nil {
			_ = closeFunc()
		}
		return nil, err
	}
	d.Channel = channel
	return d, nil
}

func (d *BlockDevice) Name() string {
	return d.name
}

func (d *BlockDevice) Path() string {
	return d.path
}

func (d *BlockDevice) MountDir() string {
	return d.mountDir
}

func (d *BlockDevice) Size() int64 {
	return d.size
}

func (d *BlockDevice) DB() *mspebble.MSPebble {
	return d.db
}

func (d *BlockDevice) Close() error {
	var err error
	if d.Channel != nil {
		err = d.Channel.Close()
	} else if d.file != nil {
		err = d.file.Close()
	}
	if d.closeFunc != nil {
		if closeErr := d.closeFunc(); err == nil {
			err = closeErr
		}
	}
	return err
}

func (d *BlockDevice) Alloc(size int64) ([]Result, []Result, error) {
	if size <= 0 {
		return nil, nil, errors.New("alloc size must be > 0")
	}

	blocks := fitBlock(size) / BlockSize

	allocBlocks, _ := d.allocator.Allocate(blocks)

	if len(allocBlocks) == 0 {
		return nil, nil, fmt.Errorf("lun %s has no enough space", d.name)
	}

	results := make([]Result, 0, len(allocBlocks))
	for _, r := range allocBlocks {
		results = append(results, Result{
			Offset: r.Offset * BlockSize,
			Size:   r.Size * BlockSize,
		})
	}
	return results, allocBlocks, nil
	// return mergeResults(results), allocBlocks, nil
}

func (d *BlockDevice) RollbackAlloc(allocBlocks []Result) {
	if len(allocBlocks) == 0 {
		return
	}
	d.allocator.Release(allocBlocks)
}

func (d *BlockDevice) Release(ctx context.Context, b *mspebble.WriteBatch, results []Result) error {
	d.bitmapMu.Lock()
	err := d.persistSpaceBits(ctx, b, results, false)
	d.bitmapMu.Unlock()
	if err != nil {
		return err
	}
	d.allocator.Release(byteResultsToBlockResults(results))
	return nil
}

func (d *BlockDevice) WriteAt(p []byte, offset int64) (int, error) {
	return d.writeAtFull(p, offset)
}

func (d *BlockDevice) ReadAt(p []byte, offset int64) (int, error) {
	return d.file.ReadAt(p, offset)
}

func (d *BlockDevice) Sync() error {
	if d.file == nil {
		return errors.New("block device file is closed")
	}
	return unix.Fsync(int(d.file.Fd()))
}

func (d *BlockDevice) PersistAllocBatch(ctx context.Context, b *mspebble.WriteBatch, results []Result) error {
	// d.bitmapMu.Lock()
	// defer d.bitmapMu.Unlock()

	return d.persistSpaceMerges(b, results, true)
}

func (d *BlockDevice) PersistBitmapBatch(_ *mspebble.WriteBatch) error {
	return errors.New("PersistBitmapBatch is obsolete; use PersistAllocBatch with allocation results")
}

func (d *BlockDevice) tryInitBlockSpace(_ context.Context) error {
	first, err := d.get(familySpaceKey(0))
	if err != nil {
		return err
	}
	if first != nil {
		return nil
	}

	total := d.spaceKeyCount()
	zero := make([]byte, SpaceLen)
	for start := int64(0); start < total; start += 1000 {
		end := start + 1000
		if end > total {
			end = total
		}
		done := mspebble.CustomizeOperateData(d.name, func(_ *pebble.DB, b *mspebble.WriteBatch, _ *mspebble.BatchRequest) error {
			for i := start; i < end; i++ {
				if err := b.Set(familySpaceKey(i), zero); err != nil {
					return err
				}
			}
			return nil
		})
		if err = <-done; err != nil {
			return err
		}
	}
	return nil
}

func (d *BlockDevice) loadAllocator(ctx context.Context) error {
	var runStart int64 = -1
	var runLen int64
	closeRun := func() error {
		if runStart < 0 {
			return nil
		}
		err := d.allocator.InitAllocated(runStart, runLen)
		runStart = -1
		runLen = 0
		return err
	}

	totalBlocks := d.size / BlockSize
	for spaceIndex := int64(0); spaceIndex < d.spaceKeyCount(); spaceIndex++ {
		value, err := d.loadSpaceValue(ctx, spaceIndex, nil)
		if err != nil {
			return err
		}
		spaceStart := spaceIndex * blocksPerSpace
		for byteIndex, b := range value {
			block := spaceStart + int64(byteIndex*8)
			if block >= totalBlocks {
				break
			}

			validBits := int64(8)
			if block+validBits > totalBlocks {
				validBits = totalBlocks - block
			}

			if b == 0 {
				if err := closeRun(); err != nil {
					return err
				}
				continue
			}
			if b == 0xff && validBits == 8 {
				if runStart < 0 {
					runStart = block
				}
				runLen += 8
				continue
			}

			for bit := int64(0); bit < validBits; bit++ {
				allocated := b&(1<<uint(7-bit)) != 0
				if allocated {
					if runStart < 0 {
						runStart = block + bit
					}
					runLen++
					continue
				}
				if err := closeRun(); err != nil {
					return err
				}
			}
		}
	}
	return closeRun()
}

func (d *BlockDevice) persistSpaceBits(ctx context.Context, b *mspebble.WriteBatch, results []Result, allocated bool) error {
	updates := make(map[int64][]byte)
	for _, r := range results {
		if r.Offset < 0 || r.Size <= 0 || r.Offset%BlockSize != 0 {
			return fmt.Errorf("invalid alloc result: %+v", r)
		}
		startBlock := r.Offset / BlockSize
		blocks := fitBlock(r.Size) / BlockSize
		for blocks > 0 {
			spaceIndex := startBlock / blocksPerSpace
			spaceOffset := startBlock % blocksPerSpace
			n := blocksPerSpace - spaceOffset
			if n > blocks {
				n = blocks
			}

			value, err := d.loadSpaceValue(ctx, spaceIndex, updates)
			if err != nil {
				return err
			}
			setSpaceBits(value, spaceOffset, n, allocated)
			updates[spaceIndex] = value

			startBlock += n
			blocks -= n
		}
	}

	for spaceIndex, value := range updates {
		if err := b.Set(familySpaceKey(spaceIndex), value); err != nil {
			return err
		}
	}
	return nil
}

func (d *BlockDevice) persistSpaceMerges(b *mspebble.WriteBatch, results []Result, allocated bool) error {
	for _, r := range results {
		if r.Offset < 0 || r.Size <= 0 || r.Offset%BlockSize != 0 {
			return fmt.Errorf("invalid alloc result: %+v", r)
		}
		values, err := spaceUpdateValues(r.Offset, r.Size, allocated)
		if err != nil {
			return err
		}
		for i, value := range values {
			key := familySpaceKey(r.Offset/SpaceSize + int64(i))
			if err := b.Merge(key, value); err != nil {
				return err
			}
		}
	}
	return nil
}

func (d *BlockDevice) loadSpaceValue(ctx context.Context, spaceIndex int64, updates map[int64][]byte) ([]byte, error) {
	if updates != nil {
		if value, ok := updates[spaceIndex]; ok {
			return value, nil
		}
	}

	value, err := d.get(familySpaceKey(spaceIndex))
	if err != nil {
		return nil, err
	}
	return normalizeSpaceValue(value), nil
}

func (d *BlockDevice) get(key []byte) ([]byte, error) {
	v, closer, err := d.db.Get(key)
	if err != nil {
		return nil, err
	}
	if v == nil {
		return nil, nil
	}
	defer closer.Close()

	out := make([]byte, len(v))
	copy(out, v)
	return out, nil
}

func (d *BlockDevice) spaceKeyCount() int64 {
	return (d.size + SpaceSize - 1) / SpaceSize
}

func (d *BlockDevice) writeAtFull(p []byte, offset int64) (int, error) {
	written := 0
	for written < len(p) {
		n, err := d.file.WriteAt(p[written:], offset+int64(written))
		written += n
		if err != nil {
			return written, err
		}
		if n == 0 {
			return written, errors.New("short write")
		}
	}
	return written, nil
}

func fitBlock(size int64) int64 {
	if size%BlockSize == 0 {
		return size
	}
	return (size/BlockSize + 1) * BlockSize
}

// 合并alloc结果中相邻的块为一个连续的范围。例如如果有两个结果分别是{Offset: 0, Size: 4096}和{Offset: 4096, Size: 4096}，
// 它们是相邻的块，可以合并为{Offset: 0, Size: 8192}。这样可以减少结果的数量，方便后续处理。
func mergeResults(in []Result) []Result {
	if len(in) == 0 {
		return nil
	}
	out := make([]Result, 0, len(in))
	cur := in[0]
	for i := 1; i < len(in); i++ {
		n := in[i]
		if cur.Offset+cur.Size == n.Offset {
			cur.Size += n.Size
			continue
		}
		out = append(out, cur)
		cur = n
	}
	out = append(out, cur)
	return out
}

func byteResultsToBlockResults(results []Result) []Result {
	blockResults := make([]Result, 0, len(results))
	for _, r := range results {
		blockResults = append(blockResults, Result{
			Offset: r.Offset / BlockSize,
			Size:   fitBlock(r.Size) / BlockSize,
		})
	}
	return blockResults
}

func familySpaceKey(index int64) []byte {
	return []byte(fmt.Sprintf("%s%010d", mspebble.RocksFileSystemPrefixOffset, index))
}

func spaceUpdateValues(offset, size int64, allocated bool) ([][]byte, error) {
	if offset < 0 || size <= 0 || offset%BlockSize != 0 {
		return nil, fmt.Errorf("invalid bitmap update offset=%d size=%d", offset, size)
	}

	keyStart := offset / SpaceSize
	keyOffset := (offset + size) / SpaceSize
	if (offset+size)%SpaceSize == 0 {
		keyOffset--
	}

	markStart := int(offset-keyStart*SpaceSize) / int(BlockSize) / 8
	markBitStartIndex := int(offset-keyStart*SpaceSize) / int(BlockSize) % 8

	keyEnd := keyOffset
	markBitEndIndex := int((size + offset - keyOffset*SpaceSize) / BlockSize % 8)
	markEnd := int((size + offset - keyOffset*SpaceSize) / BlockSize / 8)
	if size%BlockSize > 0 {
		markBitEndIndex++
	}
	if markBitEndIndex == 0 {
		if markEnd == 0 {
			keyEnd--
		}
		markBitEndIndex = 8
		markEnd--
	}

	values := make([][]byte, 0, keyEnd-keyStart+1)
	if keyStart == keyEnd {
		values = append(values, simpleSpaceUpdateValue(markStart, markEnd, markBitStartIndex, markBitEndIndex, allocated))
		return values, nil
	}

	for start := keyStart; start <= keyEnd; start++ {
		switch {
		case start == keyStart:
			values = append(values, simpleSpaceUpdateValue(markStart, SpaceLen-1, markBitStartIndex, 8, allocated))
		case start != keyEnd:
			values = append(values, simpleSpaceUpdateValue(0, SpaceLen-1, 0, 8, allocated))
		default:
			values = append(values, simpleSpaceUpdateValue(0, markEnd, 0, markBitEndIndex, allocated))
		}
	}
	return values, nil
}

func simpleSpaceUpdateValue(markStart, markEnd, markBitStartIndex, markBitEndIndex int, allocated bool) []byte {
	value := make([]byte, SpaceLen*2)
	for i := SpaceLen; i < SpaceLen*2; i++ {
		value[i] = 0xff
	}

	if allocated {
		if markEnd == markStart {
			for i := markBitEndIndex - 1; i >= markBitStartIndex; i-- {
				value[markStart] |= byte(1 << uint(7-i))
			}
			return value
		}
		for i := markStart; i <= markEnd; i++ {
			if i != markStart && i != markEnd {
				value[i] = 0xff
				continue
			}
			if i == markStart {
				for j := 7; j >= markBitStartIndex; j-- {
					value[i] |= byte(1 << uint(7-j))
				}
			} else {
				for j := 0; j < markBitEndIndex; j++ {
					value[i] |= byte(1 << uint(7-j))
				}
			}
		}
		return value
	}

	if markEnd == markStart {
		for i := markBitEndIndex - 1; i >= markBitStartIndex; i-- {
			value[markStart+SpaceLen] ^= byte(1 << uint(7-i))
		}
		return value
	}
	for i := markStart; i <= markEnd; i++ {
		if i != markStart && i != markEnd {
			value[i+SpaceLen] = 0
			continue
		}
		if i == markStart {
			for j := 7; j >= markBitStartIndex; j-- {
				value[i+SpaceLen] ^= byte(1 << uint(7-j))
			}
		} else {
			for j := 0; j < markBitEndIndex; j++ {
				value[i+SpaceLen] ^= byte(1 << uint(7-j))
			}
		}
	}
	return value
}

func normalizeSpaceValue(value []byte) []byte {
	out := make([]byte, SpaceLen)
	if len(value) == 0 {
		return out
	}
	copy(out, value)
	return out
}

func setSpaceBits(value []byte, startBlock, blocks int64, allocated bool) {
	for i := int64(0); i < blocks; i++ {
		bit := startBlock + i
		byteIndex := bit / 8
		bitIndex := uint(7 - bit%8)
		if allocated {
			value[byteIndex] |= 1 << bitIndex
		} else {
			value[byteIndex] &^= 1 << bitIndex
		}
	}
}

func EncodeResults(results []Result) ([]byte, []byte) {
	o := make([]byte, len(results)*8)
	l := make([]byte, len(results)*8)
	for i, r := range results {
		binary.LittleEndian.PutUint64(o[i*8:], uint64(r.Offset))
		binary.LittleEndian.PutUint64(l[i*8:], uint64(r.Size))
	}
	return o, l
}
