// Package mspebble provides MSPebble and BatchPebble, the Go/Pebble replacements
// for the Java MSRocksDB and BatchRocksDB classes.
//
// Design notes:
//   - Only data-pool DBs are implemented. Index DB, record DB, sync-record DB,
//     media-record DB, and rabbitmq-record DB are reserved for future work.
//   - Pebble has no column-family concept. Java's ".1." prefix column family
//     (ROCKS_FILE_SYSTEM_PREFIX_OFFSET) is already emulated via key prefixes
//     in the Go project; no new design is needed here.
//   - Java MSRocksDB used TransactionDB, but the BatchRocksDB write path never
//     used beginTransaction() / transaction locks / conflict detection.
//     MSPebble therefore does not implement transaction semantics.
package mspebble // import "Corgi/internal/database/pebble"

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/bloom"
)

// ─── Package-wide constants ──────────────────────────────────────────────────

const (
	// PebbleDirName is the sub-directory used under a mount point.
	// Intentionally matches the existing deployment convention ("pepple" typo kept).
	PebbleDirName = "pepple"

	// RocksFileSystemPrefixOffset is the ".1." key prefix that emulates Java's
	// ROCKS_FILE_SYSTEM_PREFIX_OFFSET column family.
	RocksFileSystemPrefixOffset = ".1."

	// SpaceLen is the byte length of one space-bitmap value.
	//   SpaceSize(16 MiB) / BlockSize(4 KiB) / 8 bits = 512 bytes
	SpaceLen = 512
)

// ─── MossMerger ──────────────────────────────────────────────────────────────

// newMossMerger returns a *pebble.Merger that replicates the Java/C++
// MossOperator (AssociativeMergeOperator). Pass it via opts.Merger so that
// db.Merge(key, value) behaves identically to the C++ operator.
//
// Three key patterns are handled (same as moss.cc):
//
//  1. 8-byte keys with 8-byte values → little-endian uint64 addition.
//     Used for counters: file_num, file_size, used_size, etc.
//
//  2. ".1." key prefix (RocksFileSystemPrefixOffset) → space-bitmap merge.
//     incoming = [lo | hi]  (2 × SpaceLen bytes)
//     lo = incoming[0:SpaceLen]          bits to set (allocate)
//     hi = incoming[SpaceLen:2*SpaceLen] AND mask    (release)
//
//  3. '*', '-', '0'-'9' key prefix → JSON metadata merge.
//     Keeps the entry with the larger "versionNum" JSON field.
//     When the higher-versionNum entry has "deleteMark", the other wins.
//     Dual-active "syncStamp" is checked first when present (≥2 hyphens).
func newMossMerger() *pebble.Merger {
	return &pebble.Merger{
		Name: "MossOperator",
		Merge: func(key, value []byte) (pebble.ValueMerger, error) {
			k := make([]byte, len(key))
			copy(k, key)
			v := make([]byte, len(value))
			copy(v, value)
			return &mossValueMerger{key: k, value: v}, nil
		},
	}
}

func NewMossMerger() *pebble.Merger {
	return newMossMerger()
}

type mossValueMerger struct {
	key   []byte
	value []byte
}

// MergeNewer merges a newer operand into the accumulated state.
// vm.value is the older accumulated value; value is the newer operand.
func (vm *mossValueMerger) MergeNewer(value []byte) error {
	merged, err := mossMergeValues(vm.key, vm.value, value)
	if err != nil {
		return err
	}
	vm.value = merged
	return nil
}

// MergeOlder merges an older operand into the accumulated state.
// value is the older operand; vm.value is the accumulated newer state.
func (vm *mossValueMerger) MergeOlder(value []byte) error {
	merged, err := mossMergeValues(vm.key, value, vm.value)
	if err != nil {
		return err
	}
	vm.value = merged
	return nil
}

// Finish returns the final merged value. No closer is needed.
func (vm *mossValueMerger) Finish(_ bool) ([]byte, io.Closer, error) {
	return vm.value, nil, nil
}

// mossMergeValues is the Go port of C++ MossOperator::Merge().
// existing is the current stored/accumulated value; incoming is the new operand.
func mossMergeValues(key, existing, incoming []byte) ([]byte, error) {
	// Case 1: uint64 addition — both values exactly 8 bytes.
	if len(existing) == 8 && len(incoming) == 8 {
		orig := binary.LittleEndian.Uint64(existing)
		op := binary.LittleEndian.Uint64(incoming)
		result := make([]byte, 8)
		binary.LittleEndian.PutUint64(result, orig+op)
		return result, nil
	}

	// Case 2: bitmap merge for ".1." prefix keys.
	if len(key) > 3 && key[0] == '.' && key[1] == '1' && key[2] == '.' {
		return mossBitmapMerge(existing, incoming)
	}

	// Case 3: JSON metadata merge for '*', '-', or digit prefix keys.
	if len(key) > 1 && (key[0] == '*' || key[0] == '-' || (key[0] >= '0' && key[0] <= '9')) {
		return mossJSONMerge(existing, incoming), nil
	}

	// Default: incoming replaces existing.
	result := make([]byte, len(incoming))
	copy(result, incoming)
	return result, nil
}

// mossBitmapMerge applies the C++ MossOperator bitmap branch.
//
// incoming must be 2×SpaceLen bytes: [lo | hi]
//
//	lo = incoming[0:SpaceLen]          bits to set (allocate)
//	hi = incoming[SpaceLen:2*SpaceLen] AND mask    (release)
//
// Two cases matching moss.cc exactly:
//
//	Case 1 – existing is SpaceLen bytes (base or previously compacted):
//	  result[i] = (existing[i] | lo[i]) & hi[i]
//
//	Case 2 – existing is 2×SpaceLen bytes (composed intermediate state):
//	  ci = existing[i]          | incoming[i]          (= lo[i])
//	  cj = existing[i+SpaceLen] & incoming[i+SpaceLen] (= hi[i])
//	  result[i]          = ci & cj
//	  result[i+SpaceLen] = ci | cj
func mossBitmapMerge(existing, incoming []byte) ([]byte, error) {
	if len(incoming) != 2*SpaceLen {
		return nil, fmt.Errorf("mossBitmapMerge: incoming must be %d bytes, got %d",
			2*SpaceLen, len(incoming))
	}
	result := make([]byte, len(existing))
	switch len(existing) {
	case SpaceLen:
		lo, hi := incoming[:SpaceLen], incoming[SpaceLen:]
		for i := 0; i < SpaceLen; i++ {
			result[i] = (existing[i] | lo[i]) & hi[i]
		}
	case 2 * SpaceLen:
		for i := 0; i < SpaceLen; i++ {
			j := i + SpaceLen
			ci := existing[i] | incoming[i]
			cj := existing[j] & incoming[j]
			result[i] = ci & cj
			result[j] = ci | cj
		}
	default:
		return nil, fmt.Errorf("mossBitmapMerge: existing must be %d or %d bytes, got %d",
			SpaceLen, 2*SpaceLen, len(existing))
	}
	return result, nil
}

// mossJSONMerge implements the JSON metadata merge branch of C++ MossOperator.
//
// Priority order (mirrors moss.cc exactly):
//  1. If incoming "syncStamp" has ≥2 hyphens and stamps differ:
//     keep the value with the greater syncStamp.
//  2. Otherwise compare "versionNum": keep the value with the greater version.
//     Exception: if the higher-versionNum value has "deleteMark", the other wins.
//
// Note: the C++ recordMerge() side-effect (logging conflicts to a secondary DB
// for dual-active HA sync) is not implemented here.
func mossJSONMerge(existing, incoming []byte) []byte {
	// Dual-active syncStamp comparison.
	newSyncStamp := extractJSONField(incoming, `"syncStamp":`)
	if countHyphen(newSyncStamp) >= 2 {
		oldSyncStamp := extractJSONField(existing, `"syncStamp":`)
		if cmp := compareSyncStamp(oldSyncStamp, newSyncStamp); cmp != 0 {
			winner := existing
			if cmp > 0 { // newSyncStamp is greater → incoming wins
				winner = incoming
			}
			out := make([]byte, len(winner))
			copy(out, winner)
			return out
		}
	}

	// versionNum comparison.
	oldVersion := extractJSONField(existing, `"versionNum":`)
	newVersion := extractJSONField(incoming, `"versionNum":`)
	cmp := compareVersion(oldVersion, newVersion)
	var winner []byte
	switch {
	case cmp == 0:
		winner = existing // equal versions → keep existing
	case cmp > 0:
		winner = incoming // incoming has higher versionNum
	default:
		// existing has higher versionNum; if existing has deleteMark, incoming wins
		if hasJSONKey(existing, `"deleteMark":`) {
			winner = incoming
		} else {
			winner = existing
		}
	}
	out := make([]byte, len(winner))
	copy(out, winner)
	return out
}

// ─── JSON helpers (Go port of C++ MossOperator private methods) ──────────────

// extractJSONField finds name in data and returns the unquoted string value
// that follows it. Returns nil when not found or value is not a quoted string.
// name must include the trailing colon, e.g., `"versionNum":`.
// Port of C++ MossOperator::getJson().
func extractJSONField(data []byte, name string) []byte {
	nb := []byte(name)
	match := 0
	start := -1
	for i := 0; i < len(data); i++ {
		if data[i] == nb[match] {
			match++
			if match == len(nb) {
				start = i // index of last byte of matched name (the ':')
				break
			}
		} else {
			match = 0
		}
	}
	if start < 0 {
		return nil
	}
	end := -1
	for i := start; i < len(data); i++ {
		if data[i] == ',' || data[i] == '}' {
			end = i
			break
		}
	}
	// Require closing quote and room for at least one character value.
	if end <= start || data[end-1] != '"' || start+2 >= end-1 {
		return nil
	}
	// Extract data[start+2 : end-1] (strip opening and closing quotes).
	result := make([]byte, end-1-(start+2))
	copy(result, data[start+2:end-1])
	return result
}

// hasJSONKey returns true when name appears anywhere in data.
// Port of C++ MossOperator::keyExistsForJson().
func hasJSONKey(data []byte, name string) bool {
	nb := []byte(name)
	match := 0
	for i := 0; i < len(data); i++ {
		if data[i] == nb[match] {
			match++
			if match == len(nb) {
				return true
			}
		} else {
			match = 0
		}
	}
	return false
}

// countHyphen counts '-' bytes in s.
func countHyphen(s []byte) int {
	n := 0
	for _, b := range s {
		if b == '-' {
			n++
		}
	}
	return n
}

// compareVersion compares two extracted version strings lexicographically.
// Returns positive when v2 > v1, negative when v1 > v2, 0 when equal.
// Port of C++ MossOperator::compareVersion().
func compareVersion(v1, v2 []byte) int {
	if v1 == nil && v2 == nil {
		return 0
	}
	if v1 == nil {
		return 1 // v2 exists → v2 > v1
	}
	if v2 == nil {
		return -1 // v1 exists → v1 > v2
	}
	for i := 0; i < len(v1) && i < len(v2); i++ {
		if c := int(v2[i]) - int(v1[i]); c != 0 {
			return c
		}
	}
	switch {
	case len(v1) == len(v2):
		return 0
	case len(v1) < len(v2):
		return 1 // v2 is longer → v2 > v1
	default:
		return -1 // v1 is longer → v1 > v2
	}
}

// compareSyncStamp compares two syncStamp strings.
// Old-format values with fewer than 2 hyphens are always treated as smaller.
// Port of C++ MossOperator::compareSyncStamp().
func compareSyncStamp(v1, v2 []byte) int {
	if v1 == nil && v2 == nil {
		return 0
	}
	if v1 == nil {
		return 1
	}
	if v2 == nil {
		return -1
	}
	if countHyphen(v1) < 2 {
		return 1 // old-format v1 is always less than new-format v2
	}
	return compareVersion(v1, v2)
}

// ─── MSPebble ────────────────────────────────────────────────────────────────

// MSPebble is the Go/Pebble equivalent of the Java MSRocksDB subset used by
// the BlockDevice data path.
//
// Column-family note:
//
//	Java MSRocksDB opened three column families:
//	  default                         → regular metadata keys
//	  ".1." (ROCKS_FILE_SYSTEM_PREFIX_OFFSET) → bitmap space keys
//	  "migrate"                        → migration path (not needed for data path)
//
//	Pebble has no column-family concept. ".1." key prefix routing is already
//	implemented in the Go project. No further changes are required here.
//
// Transaction note:
//
//	Java MSRocksDB used TransactionDB. The BatchRocksDB fast path never called
//	beginTransaction(), so MSPebble skips transaction support entirely.
type MSPebble struct {
	Lun    string
	Path   string
	DB     *pebble.DB
	closed atomic.Bool
}

// OpenDataDB opens (creating if needed) a Pebble data-pool DB for lun at
// <mountDir>/<PebbleDirName>.
//
// Callers are responsible for registering the returned *MSPebble with
// AddDataBatch() if they need the async batch-write framework.
func OpenDataDB(lun, mountDir string) (*MSPebble, error) {
	path := filepath.Join(mountDir, PebbleDirName)
	if err := os.MkdirAll(path, 0o755); err != nil {
		return nil, err
	}

	opts := buildDataOptions()
	db, err := pebble.Open(path, opts)
	if err != nil {
		return nil, err
	}
	return &MSPebble{
		Lun:  lun,
		Path: path,
		DB:   db,
	}, nil
}

// buildDataOptions returns Pebble options calibrated to the Java MSRocksDB
// openRocks() configuration for a data-pool (non-index) DB.
//
// ─── Java → Pebble parameter mapping ───────────────────────────────────────
//
//	createIfMissing(true)
//	  → Pebble always creates the DB if the path is absent. No option needed.
//
//	keepLogFileNum(10) / maxLogFileSize(1 << 30)
//	  → Pebble does not expose WAL log-rotation limits via public Options.
//	    WAL files are recycled internally (controlled by MaxNumRecyclableLogs,
//	    which is derived from MemTableStopWritesThreshold). Provide a custom
//	    Logger via Options.Logger to redirect log output. No direct equivalent.
//
//	maxBackgroundJobs(8) / maxSubcompactions(8)
//	  → Pebble has no MaxConcurrentCompactions public field. Compaction worker
//	    count grows automatically based on compaction debt. The threshold at
//	    which an additional compaction goroutine is spawned is controlled by
//	    Experimental.CompactionDebtConcurrency (default 1 GiB). We leave this
//	    at the default and let Pebble scale compaction workers dynamically.
//
//	level0FileNumCompactionTrigger(1)
//	  → L0CompactionThreshold = 1
//
//	disableAutoCompactions(false)
//	  → Pebble always runs background compactions; there is no public option to
//	    disable them.
//
//	targetFileSizeBase(256 MiB)
//	  → TargetFileSizes[0] (L0) and TargetFileSizes[1] (Lbase) = 256 MiB.
//	    EnsureDefaults() automatically sets [2]..[6] to [i-1]×2.
//
//	maxBytesForLevelBase = targetFileSizeBase × 4 = 1 GiB
//	  → Pebble has no MaxBytesForLevelBase field. The effective Lbase total-
//	    size cap is approximately:
//	      L0CompactionThreshold × TargetFileSizes[1] × LevelMultiplier
//	      = 1 × 256 MiB × 4 = 1 GiB
//	    We set Experimental.LevelMultiplier = 4 to match.
//
//	maxWriteBufferNumber(4)
//	  → MemTableStopWritesThreshold = 4
//
//	BloomFilter(10 bits/key)
//	  → Levels[0].FilterPolicy = bloom.FilterPolicy(10).
//	    EnsureL1PlusDefaults() propagates this filter to L1–L6 automatically.
//
//	block cache
//	  → CacheSize = 128 MiB (data DB; index DB would use 512 MiB).
//
//	maxOpenFiles(512) for data DB
//	  → MaxOpenFiles = 512
//	    (index DB would use 4096; implement a separate OpenIndexDB() when needed)
//
//	RateLimiter(1<<32, …)
//	  → Pebble does not have a built-in write-rate limiter equivalent. If rate
//	    limiting is required, apply it at the application layer before calling
//	    BatchPebble.
func buildDataOptions() *pebble.Options {
	opts := &pebble.Options{
		// Block cache.
		CacheSize: 128 << 20, // 128 MiB

		// Memtable configuration.
		// Java: maxWriteBufferNumber(4) → stop writes when 4 memtables are full.
		MemTableSize:                64 << 20, // 64 MiB per memtable
		MemTableStopWritesThreshold: 4,

		// L0 compaction trigger.
		// Java: level0FileNumCompactionTrigger(1)
		L0CompactionThreshold: 1,
		// Keep Pebble's default stop-writes threshold (12) to avoid stalling
		// on transient L0 spikes.
		L0StopWritesThreshold: 12,

		// Target SSTable file sizes.
		// Java: targetFileSizeBase(256 MiB) → L0 and Lbase both 256 MiB.
		// EnsureDefaults() fills [2]..[6] by doubling the previous level.
		// TargetFileSizes is [manifest.NumLevels]int64 = [7]int64.

		// Sync intervals – smooth out bursts without sacrificing durability.
		BytesPerSync:    1 << 20, // 1 MiB
		WALBytesPerSync: 1 << 20,

		// Open file-descriptor limit.
		// Java: maxOpenFiles(512) for data DB.
		MaxOpenFiles: 512,
	}

	// TargetFileSizes: set index 0 (L0) and 1 (Lbase) to 256 MiB.
	// Higher levels are filled automatically by pebble.Options.EnsureDefaults().
	opts.TargetFileSizes[0] = 256 << 20 // L0:    256 MiB
	opts.TargetFileSizes[1] = 256 << 20 // Lbase: 256 MiB  (= Java targetFileSizeBase)

	// Bloom filter on all levels.
	// Setting Levels[0] is sufficient; EnsureL1PlusDefaults() propagates the
	// filter policy to L1–L6 when their FilterPolicy field is nil.
	opts.Levels[0] = pebble.LevelOptions{
		FilterPolicy: bloom.FilterPolicy(10), // 10 bits/key, matching Java BloomFilter(10, false)
	}

	// LevelMultiplier = 4 gives effective Lbase total-size ≈
	//   L0CompactionThreshold × TargetFileSizes[1] × LevelMultiplier
	//   = 1 × 256 MiB × 4 = 1 GiB
	// This matches Java maxBytesForLevelBase = targetFileSizeBase × 4.
	opts.Experimental.LevelMultiplier = 4

	// Merger: replicates the Java/C++ MossOperator associative merge semantics.
	// Required for db.Merge(key, value) to work correctly.
	opts.Merger = newMossMerger()

	return opts
}

// ─── MSPebble methods ────────────────────────────────────────────────────────

// Close closes the underlying Pebble DB. Safe to call from any goroutine;
// subsequent calls return an error immediately.
func (m *MSPebble) Close() error {
	if m.closed.Swap(true) {
		return errors.New("MSPebble already closed")
	}
	return m.DB.Close()
}

// Get reads the value for key. The returned io.Closer must be closed by the
// caller to release the underlying buffer. Returns (nil, nil, nil) when the
// key does not exist.
func (m *MSPebble) Get(key []byte) ([]byte, io.Closer, error) {
	if m.closed.Load() {
		return nil, nil, errors.New("MSPebble is closed")
	}
	v, closer, err := m.DB.Get(key)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return nil, nil, nil
		}
		return nil, nil, err
	}
	return v, closer, nil
}

// Set writes key → value. When sync is true the write is fsync'd before
// returning, matching Java WRITE_OPTIONS.setSync(true).
func (m *MSPebble) Set(key, value []byte, sync bool) error {
	if m.closed.Load() {
		return errors.New("MSPebble is closed")
	}
	wo := pebble.NoSync
	if sync {
		wo = pebble.Sync
	}
	return m.DB.Set(key, value, wo)
}

// Delete removes key. When sync is true the delete is fsync'd before returning.
func (m *MSPebble) Delete(key []byte, sync bool) error {
	if m.closed.Load() {
		return errors.New("MSPebble is closed")
	}
	wo := pebble.NoSync
	if sync {
		wo = pebble.Sync
	}
	return m.DB.Delete(key, wo)
}

func (m *MSPebble) Merge(key, value []byte, sync bool) error {
	if m.closed.Load() {
		return errors.New("MSPebble is closed")
	}
	wo := pebble.NoSync
	if sync {
		wo = pebble.Sync
	}
	return m.DB.Merge(key, value, wo)
}
