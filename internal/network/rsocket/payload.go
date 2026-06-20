package rsocket

import (
	"encoding/binary"
	"fmt"
	"google.golang.org/protobuf/proto"
	"Corgi/internal/message/pb"
	"strings"

	rspayload "github.com/rsocket/rsocket-go/payload"
)

type PayloadMetaType string

const (
	Error                PayloadMetaType = "ERROR"
	Success              PayloadMetaType = "SUCCESS"
	Continue             PayloadMetaType = "CONTINUE"
	StartPutObject       PayloadMetaType = "START_PUT_OBJECT"
	PutObject            PayloadMetaType = "PUT_OBJECT"
	CompletePutObject    PayloadMetaType = "COMPLETE_PUT_OBJECT"
	PutAndCompleteObject PayloadMetaType = "PUT_AND_COMPLETE_PUT_OBJECT"
	StartPartUpload      PayloadMetaType = "START_PART_UPLOAD"
	PartUpload           PayloadMetaType = "PART_UPLOAD"
	CompletePartUpload   PayloadMetaType = "COMPLETE_PART_UPLOAD"
	StartBatchPut        PayloadMetaType = "START_BATCH_PUT"
	CPSSFile             PayloadMetaType = "CP_SST_FILE"
	StartFileAsync       PayloadMetaType = "START_FILE_ASYNC_CHANNEL"
)

type RequestPayload struct {
	MetaType PayloadMetaType
	Data     []byte
}

type ResponsePayload struct {
	MetaType PayloadMetaType
	Err      error
}

// DecodePayload converts a raw RSocket payload into the request shape used by
// the object upload handler. Metadata follows the Java server convention:
// the enum name is sent as plain UTF-8 bytes.
func DecodePayload(p rspayload.Payload) (RequestPayload, error) {
	meta, ok := p.MetadataUTF8()
	if !ok || strings.TrimSpace(meta) == "" {
		return RequestPayload{}, fmt.Errorf("missing payload metadata")
	}

	req := RequestPayload{
		MetaType: PayloadMetaType(strings.TrimSpace(meta)),
		Data:     p.Data(), //直接引用 应该不会有问题
	}

	return req, nil
}

func EncodePayload(resp ResponsePayload) rspayload.Payload {
	var data []byte
	if resp.Err != nil {
		data = []byte(resp.Err.Error())
	}
	return rspayload.New(data, []byte(resp.MetaType))
}

func decodePutAndComplete(data []byte) (*pb.PutInitRequest, []byte, error) {
	if len(data) < 8 {
		return nil, nil, fmt.Errorf("%s payload too short: %d", PutAndCompleteObject, len(data))
	}

	startLen64 := binary.LittleEndian.Uint64(data[:8])
	if startLen64 > uint64(len(data)-8) {
		return nil, nil, fmt.Errorf(
			"%s start metadata length %d exceeds payload size %d",
			PutAndCompleteObject,
			startLen64,
			len(data),
		)
	}

	startLen := int(startLen64)
	startBytes := data[8 : 8+startLen]
	body := data[8+startLen:] // 直接引用，不 clone

	start, err := decodePutInitRequest(startBytes)
	if err != nil {
		return nil, nil, err
	}

	return start, body, nil
}

func decodePutInitRequest(data []byte) (*pb.PutInitRequest, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("empty PutInitRequest payload")
	}

	var req pb.PutInitRequest
	if err := proto.Unmarshal(data, &req); err != nil {
		return nil, fmt.Errorf("unmarshal PutInitRequest failed: %w", err)
	}
	return &req, nil
}


func cloneBytes(in []byte) []byte {
	if len(in) == 0 {
		return nil
	}
	out := make([]byte, len(in))
	copy(out, in)
	return out
}
