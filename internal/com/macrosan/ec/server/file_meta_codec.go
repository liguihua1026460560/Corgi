package server

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"mossserver/internal/com/macrosan/message/pb"
)

func encodeFileMetaBinary(m *pb.FileMeta) ([]byte, error) {
	buf := bytes.NewBuffer(make([]byte, 0, 256))
	writeString := func(s string) error {
		if err := binary.Write(buf, binary.LittleEndian, uint32(len(s))); err != nil {
			return err
		}
		_, err := buf.WriteString(s)
		return err
	}

	if err := writeString(m.MetaKey); err != nil {
		return nil, err
	}
	if err := writeString(m.FileName); err != nil {
		return nil, err
	}
	if err := writeString(m.Lun); err != nil {
		return nil, err
	}
	if err := writeString(m.Etag); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.LittleEndian, m.Size); err != nil {
		return nil, err
	}

	if len(m.Offset) != len(m.Len) {
		return nil, fmt.Errorf("offset/len size mismatch")
	}
	if err := binary.Write(buf, binary.LittleEndian, uint32(len(m.Offset))); err != nil {
		return nil, err
	}
	for i := 0; i < len(m.Offset); i++ {
		if err := binary.Write(buf, binary.LittleEndian, m.Offset[i]); err != nil {
			return nil, err
		}
		if err := binary.Write(buf, binary.LittleEndian, m.Len[i]); err != nil {
			return nil, err
		}
	}
	return buf.Bytes(), nil
}

func DecodeFileMetaBinary(data []byte) (*pb.FileMeta, error) {
	return decodeFileMetaBinary(data)
}

func decodeFileMetaBinary(data []byte) (*pb.FileMeta, error) {
	r := bytes.NewReader(data)
	readString := func() (string, error) {
		var n uint32
		if err := binary.Read(r, binary.LittleEndian, &n); err != nil {
			return "", err
		}
		b := make([]byte, n)
		if _, err := r.Read(b); err != nil {
			return "", err
		}
		return string(b), nil
	}

	metaKey, err := readString()
	if err != nil {
		return nil, err
	}
	fileName, err := readString()
	if err != nil {
		return nil, err
	}
	lun, err := readString()
	if err != nil {
		return nil, err
	}
	etag, err := readString()
	if err != nil {
		return nil, err
	}

	var size int64
	if err = binary.Read(r, binary.LittleEndian, &size); err != nil {
		return nil, err
	}

	var n uint32
	if err = binary.Read(r, binary.LittleEndian, &n); err != nil {
		return nil, err
	}
	offset := make([]int64, 0, n)
	length := make([]int64, 0, n)
	for i := uint32(0); i < n; i++ {
		var o int64
		var l int64
		if err = binary.Read(r, binary.LittleEndian, &o); err != nil {
			return nil, err
		}
		if err = binary.Read(r, binary.LittleEndian, &l); err != nil {
			return nil, err
		}
		offset = append(offset, o)
		length = append(length, l)
	}

	return &pb.FileMeta{
		MetaKey:  metaKey,
		FileName: fileName,
		Lun:      lun,
		Etag:     etag,
		Size:     size,
		Offset:   offset,
		Len:      length,
	}, nil
}
