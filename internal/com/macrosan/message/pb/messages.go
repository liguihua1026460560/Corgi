package pb

// NOTE:
// Real protobuf-generated types should replace this file.
// This placeholder keeps server flow compilable while protocol files are finalized.

type PutInitRequest struct {
	Lun      string
	FileName string
	MetaKey  string
	NoGet    bool
	Replace  bool
	Recover  bool
}

type FileMeta struct {
	MetaKey  string
	FileName string
	Lun      string
	Etag     string
	Size     int64
	Offset   []int64
	Len      []int64
}
