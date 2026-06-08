package rsocket

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"mossserver/internal/com/macrosan/message/pb"

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
	StartPut *pb.PutInitRequest
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
		Data:     cloneBytes(p.Data()),
	}

	switch req.MetaType {
	case PutAndCompleteObject:
		start, data, err := decodePutAndComplete(req.Data)
		if err != nil {
			return RequestPayload{}, err
		}
		req.StartPut = start
		req.Data = data
	case StartPutObject, StartPartUpload, StartBatchPut:
		start, err := decodePutInitRequest(req.Data)
		if err != nil {
			return RequestPayload{}, err
		}
		req.StartPut = start
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
		return nil, nil, fmt.Errorf("%s start metadata length %d exceeds payload size %d", PutAndCompleteObject, startLen64, len(data))
	}

	startLen := int(startLen64)
	start, err := decodePutInitRequest(data[8 : 8+startLen])
	if err != nil {
		return nil, nil, err
	}
	return start, cloneBytes(data[8+startLen:]), nil
}

func decodePutInitRequest(data []byte) (*pb.PutInitRequest, error) {
	fields, err := decodeSocketReqFields(data)
	if err != nil {
		return nil, err
	}

	return &pb.PutInitRequest{
		Lun:      fields["lun"],
		FileName: firstNonEmpty(fields["fileName"], fields["file_name"]),
		MetaKey:  firstNonEmpty(fields["metaKey"], fields["meta_key"]),
		NoGet:    parseBool(fields["noGet"]),
		Replace:  parseBool(fields["replace"]),
		Recover:  parseBool(fields["recover"]),
	}, nil
}

func decodeSocketReqFields(data []byte) (map[string]string, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("empty start metadata")
	}

	if fields, err := decodeSocketReqJSON(data); err == nil {
		return fields, nil
	}
	return decodeSocketReqBinary(data)
}

func decodeSocketReqJSON(data []byte) (map[string]string, error) {
	var envelope struct {
		DataMap map[string]string `json:"dataMap"`
	}
	if err := json.Unmarshal(data, &envelope); err == nil && len(envelope.DataMap) > 0 {
		return envelope.DataMap, nil
	}

	var raw map[string]any
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, err
	}

	if nested, ok := raw["dataMap"].(map[string]any); ok {
		return stringMap(nested), nil
	}
	return stringMap(raw), nil
}

func decodeSocketReqBinary(data []byte) (map[string]string, error) {
	if len(data) < 4 {
		return nil, fmt.Errorf("invalid binary SocketReqMsg: payload too short")
	}
	if data[0]&0x80 != 0 {
		return nil, fmt.Errorf("SocketDataMsg binary format is not supported")
	}

	count := int(binary.BigEndian.Uint32(data[:4]))
	if count < 0 {
		return nil, fmt.Errorf("invalid binary SocketReqMsg field count: %d", count)
	}

	fields := make(map[string]string, count)
	offset := 4
	for i := 0; i < count; i++ {
		key, next, err := readJavaBytes(data, offset)
		if err != nil {
			return nil, fmt.Errorf("invalid binary SocketReqMsg key %d: %w", i, err)
		}
		offset = next

		valueLen, next, err := readJavaInt(data, offset)
		if err != nil {
			return nil, fmt.Errorf("invalid binary SocketReqMsg value %d: %w", i, err)
		}
		offset = next

		if valueLen == -1 {
			fields[string(key)] = ""
			continue
		}
		if valueLen < 0 || offset+valueLen > len(data) {
			return nil, fmt.Errorf("invalid binary SocketReqMsg value length %d", valueLen)
		}
		fields[string(key)] = string(data[offset : offset+valueLen])
		offset += valueLen
	}

	return fields, nil
}

func readJavaBytes(data []byte, offset int) ([]byte, int, error) {
	n, next, err := readJavaInt(data, offset)
	if err != nil {
		return nil, offset, err
	}
	if n < 0 || next+n > len(data) {
		return nil, offset, fmt.Errorf("invalid length %d", n)
	}
	return data[next : next+n], next + n, nil
}

func readJavaInt(data []byte, offset int) (int, int, error) {
	if offset+4 > len(data) {
		return 0, offset, fmt.Errorf("need 4 bytes at offset %d", offset)
	}
	return int(int32(binary.BigEndian.Uint32(data[offset : offset+4]))), offset + 4, nil
}

func stringMap(in map[string]any) map[string]string {
	out := make(map[string]string, len(in))
	for k, v := range in {
		switch vv := v.(type) {
		case nil:
			out[k] = ""
		case string:
			out[k] = vv
		case bool:
			out[k] = strconv.FormatBool(vv)
		case float64:
			out[k] = strconv.FormatFloat(vv, 'f', -1, 64)
		default:
			out[k] = fmt.Sprint(vv)
		}
	}
	return out
}

func parseBool(v string) bool {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "1", "true", "yes", "y":
		return true
	default:
		return false
	}
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}

func cloneBytes(in []byte) []byte {
	if len(in) == 0 {
		return nil
	}
	out := make([]byte, len(in))
	copy(out, in)
	return out
}
