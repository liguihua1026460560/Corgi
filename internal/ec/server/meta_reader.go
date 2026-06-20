package server

import (
	"context"
	"fmt"

	mspebble "Corgi/internal/database/pebble"
	"Corgi/internal/message/pb"
)

func QueryFileMeta(_ context.Context, db *mspebble.MSPebble, metaKey string) (*pb.FileMeta, error) {
	v, closer, err := db.Get(fileMetaKey(metaKey))
	if err != nil {
		return nil, err
	}
	if closer != nil {
		defer closer.Close()
	}
	if len(v) == 0 {
		return nil, fmt.Errorf("file meta not found: %s", metaKey)
	}
	return decodeFileMetaBinary(v)
}
