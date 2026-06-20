package pb

import (
	"Corgi/internal/define"
	"strings"
)

func (m *FileMeta) GetKey() string {
	return GetFileMetaKey(m.FileName)
}

func GetFileMetaKey(fileName string) string {
	parts := strings.Split(fileName, "/")
	if len(parts) > 1 {
		return define.PebbleFileMetaPrefix + parts[1]
	}
	return define.PebbleFileMetaPrefix + fileName
}
