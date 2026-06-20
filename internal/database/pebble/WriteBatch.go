package mspebble // import "Corgi/internal/database/pebble"

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"

	"github.com/cockroachdb/pebble/v2"
)

const rocksObjMetaDeleteMarker = byte('~')

// migrateState is a placeholder for the Java MigrateServer/LocalMigrateServer
// integration. The branch conditions are kept in the write path so the real
// migration implementation can be filled in without reshaping WriteBatch.
type migrateState struct {
	start int
}

func (m *migrateState) needWriteToMigrateColumnFamily(_ string, _ []byte) bool {
	return false
}

var (
	migrate            = &migrateState{}
	localMigrateServer = &migrateState{}
)

// WriteBatch is the Pebble equivalent of Java WriteBatch backed by
// WriteBatchWithIndex. Pebble's indexed batch reads from both the pending
// batch and the DB, so reads observe writes already staged in this batch.
type WriteBatch struct {
	db    *pebble.DB
	lun   string
	batch *pebble.Batch
}

func NewWriteBatch(db *pebble.DB, lun string) *WriteBatch {
	return &WriteBatch{
		db:    db,
		lun:   lun,
		batch: db.NewIndexedBatch(),
	}
}

// GetFromBatchAndDB mirrors Java getFromBatchAndDB(). The db parameter is kept
// for source-level parity; Pebble ties an IndexedBatch to its parent DB, so the
// read is served by the batch itself.
func (w *WriteBatch) GetFromBatchAndDB(db *pebble.DB, key []byte) ([]byte, error) {
	return w.getFromBatchAndDB(db, key)
}

func (w *WriteBatch) getFromBatchAndDB(_ *pebble.DB, key []byte) ([]byte, error) {
	if len(key) > 0 && key[0] >= '0' && key[0] <= '9' {
		// 数字开头的对象元数据需要先查原始 key；如果不存在，再查 "~"+key 的 delete marker key。
		res, err := w.getCopy(key)
		if err != nil || res != nil {
			return res, err
		}
		return w.getCopy(getDeleteMarkKey(key))
	}

	return w.getCopy(key)
}

// Get keeps the Pebble Batch-style API available for custom consumers.
func (w *WriteBatch) Get(key []byte) ([]byte, io.Closer, error) {
	return w.batch.Get(key)
}

func (w *WriteBatch) Put(key, value []byte) error {
	return w.put(key, value)
}

func (w *WriteBatch) put(key, value []byte) error {
	if len(key) > 0 && key[0] >= '0' && key[0] <= '9' {
		// 数字开头的对象元数据写入前，需要删除普通 key 和对应 delete marker key，避免新旧状态并存。
		if err := w.delete(key); err != nil {
			return err
		}
		if isDeleteMarker(value) {
			// 当前 value 表示 deleteMark 时，实际写入到 "~"+key。
			key = getDeleteMarkKey(key)
		}
	}

	if isNeedMigrate(key) && migrate.needWriteToMigrateColumnFamily(w.lun, key) {
		// TODO: 迁移列族写入逻辑尚未移植；Java 版这里会同时写 migrate column family。
	}

	if migrate.start > 0 && isNeedMigrate(key) {
		// TODO: 远端迁移写入逻辑尚未移植；Java 版这里会通过 RSocket rewrite 到目标 LUN。
	}

	if localMigrateServer.start > 0 && isNeedMigrate(key) {
		// TODO: 本地迁移写入逻辑尚未移植；Java 版这里会写入目标本地 LUN。
	}

	return w.batch.Set(key, value, nil)
}

// PutWithCutting mirrors Java put(boolean cutting, byte[] key, byte[] value).
func (w *WriteBatch) PutWithCutting(cutting bool, key, value []byte) error {
	if cutting {
		// TODO: simplifyMetaJson 尚未移植；当前先按原始 value 写入。
	}
	return w.put(key, value)
}

// Set keeps the Pebble Batch-style API available for custom consumers.
func (w *WriteBatch) Set(key, value []byte, _ ...*pebble.WriteOptions) error {
	return w.put(key, value)
}

func (w *WriteBatch) Delete(key []byte, _ ...*pebble.WriteOptions) error {
	return w.delete(key)
}

func (w *WriteBatch) delete(key []byte) error {
	if len(key) > 0 && key[0] >= '0' && key[0] <= '9' {
		// 数字开头的对象元数据删除时，需要同步删除 "~"+key 的 delete marker key。
		if err := w.delete(getDeleteMarkKey(key)); err != nil {
			return err
		}
	}

	if isNeedMigrate(key) && migrate.needWriteToMigrateColumnFamily(w.lun, key) {
		// TODO: 迁移列族删除逻辑尚未移植；Java 版这里会读取旧值并写入带 "~" 前缀的删除标记。
	}

	if migrate.start > 0 && isNeedMigrate(key) {
		// TODO: 远端迁移删除逻辑尚未移植；Java 版这里会记录待迁移删除操作。
	}

	if localMigrateServer.start > 0 && isNeedMigrate(key) {
		// TODO: 本地迁移删除逻辑尚未移植；Java 版这里会记录本地迁移删除操作。
	}

	return w.batch.Delete(key, nil)
}

func (w *WriteBatch) Merge(key, value []byte, _ ...*pebble.WriteOptions) error {
	return w.merge(key, value)
}

func (w *WriteBatch) merge(key, value []byte) error {
	if isNeedMigrate(key) && migrate.needWriteToMigrateColumnFamily(w.lun, key) {
		// TODO: 迁移列族 merge 逻辑尚未移植；Java 版这里会同时 merge migrate column family。
	}

	if len(value) > 8 {
		if migrate.start > 0 && isNeedMigrate(key) {
			// TODO: 远端迁移 merge 逻辑尚未移植；Java 版这里会通过 RSocket rewrite merge 到目标 LUN。
		}

		if localMigrateServer.start > 0 && isNeedMigrate(key) {
			// TODO: 本地迁移 merge 逻辑尚未移植；Java 版这里会 merge 到目标本地 LUN。
		}
	}

	return w.batch.Merge(key, value, nil)
}

func (w *WriteBatch) TransferAndPut(key, value []byte) error {
	if err := w.delete(key); err != nil {
		return err
	}
	if len(key) == 0 {
		return w.batch.Set([]byte{rocksObjMetaDeleteMarker}, value, nil)
	}
	deleteMarker := make([]byte, len(key))
	deleteMarker[0] = rocksObjMetaDeleteMarker
	copy(deleteMarker[1:], key[1:])
	return w.batch.Set(deleteMarker, value, nil)
}

func (w *WriteBatch) RawPut(key, value []byte) error {
	return w.batch.Set(key, value, nil)
}

func (w *WriteBatch) RawDelete(key []byte) error {
	return w.batch.Delete(key, nil)
}

func (w *WriteBatch) RawMerge(key, value []byte) error {
	return w.batch.Merge(key, value, nil)
}

func (w *WriteBatch) NewIter(o *pebble.IterOptions) (*pebble.Iterator, error) {
	return w.batch.NewIter(o)
}

func (w *WriteBatch) Commit(o *pebble.WriteOptions) error {
	return w.batch.Commit(o)
}

func (w *WriteBatch) Close() error {
	return w.batch.Close()
}

func (w *WriteBatch) Clear() {
	w.batch.Reset()
}

func (w *WriteBatch) PebbleBatch() *pebble.Batch {
	return w.batch
}

func (w *WriteBatch) getCopy(key []byte) ([]byte, error) {
	v, closer, err := w.batch.Get(key)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return nil, nil
		}
		return nil, err
	}
	defer closer.Close()

	res := make([]byte, len(v))
	copy(res, v)
	return res, nil
}

func isNeedMigrate(key []byte) bool {
	if len(key) == 0 {
		return false
	}
	switch key[0] {
	case '!', '@', '*', '+', '-', '|', ':', '(', ')':
		return true
	default:
		return key[0] >= '0' && key[0] <= '9'
	}
}

func getDeleteMarkKey(key []byte) []byte {
	deleteMarker := make([]byte, 1+len(key))
	deleteMarker[0] = rocksObjMetaDeleteMarker
	copy(deleteMarker[1:], key)
	return deleteMarker
}

func isDeleteMarker(value []byte) bool {
	var meta map[string]any
	if err := json.Unmarshal(value, &meta); err == nil {
		switch v := meta["deleteMark"].(type) {
		case bool:
			return v
		case string:
			return bytes.Contains([]byte(v), []byte("true"))
		}
	}
	return bytes.Contains(value, []byte(`"deleteMark":true`)) ||
		bytes.Contains(value, []byte(`"deleteMark":"true"`))
}
