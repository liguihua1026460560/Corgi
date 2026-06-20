package rsocket

import (
	"context"
	"fmt"
	"Corgi/internal/ec/server"
	"Corgi/internal/fs"
	"Corgi/internal/message/pb"
)

type ErasureServer struct {
	device *fs.BlockDevice
}

func NewErasureServer(device *fs.BlockDevice) *ErasureServer {
	return &ErasureServer{device: device}
}

// RequestChannel simulates Java ErasureServer.requestChannel for object PUT path.
// CP_SST_FILE and START_FILE_ASYNC_CHANNEL are intentionally ignored in object-only scope.
func (s *ErasureServer) RequestChannel(ctx context.Context, payloads <-chan RequestPayload) <-chan ResponsePayload {
	out := make(chan ResponsePayload, 64)

	go func() {
		defer close(out)

		first := true
		hResp := make(chan server.Response, 64)
		var h *server.AioUploadServerHandler

		startHandler := func(req *pb.PutInitRequest) bool {
			device := s.device
			if req != nil && req.Lun != "" {
				if d := fs.Get(req.Lun); d != nil {
					device = d
				}
			}
			if device == nil {
				lun := ""
				if req != nil {
					lun = req.Lun
				}
				out <- ResponsePayload{MetaType: Error, Err: fmt.Errorf("missing block device for lun %q", lun)}
				return false
			}

			h = server.NewAioUploadServerHandler(device, hResp)
			h.Start(req)
			return true
		}

		forward := func(r server.Response) bool {
			meta := Success
			if r.Type == server.MetaContinue {
				meta = Continue
			}
			if r.Type == server.MetaError {
				meta = Error
			}
			out <- ResponsePayload{MetaType: meta, Err: r.Err}
			return r.Type != server.MetaError
		}

		for p := range payloads {
			if first {
				switch p.MetaType {
				case PutAndCompleteObject:
					start, body, err := decodePutAndComplete(p.Data)
					if err != nil {
						out <- ResponsePayload{MetaType: Error, Err: err}
						return
					}
					if !startHandler(start) {
						return
					}
					h.Put(body)
					if !forward(<-hResp) {
						return
					}
					h.Complete(ctx)
					forward(<-hResp)
					return
				case StartPutObject, StartPartUpload, StartBatchPut:
					start, err := decodePutInitRequest(p.Data)
					if err != nil {
						out <- ResponsePayload{MetaType: Error, Err: err}
						return
					}

					if !startHandler(start) {
						return
					}
				case CPSSFile, StartFileAsync:
					// Object-storage scope: ignore file-related commands as requested.
					out <- ResponsePayload{MetaType: Error, Err: fmt.Errorf("meta type %s ignored in object mode", p.MetaType)}
					return
				default:
					out <- ResponsePayload{MetaType: Error, Err: fmt.Errorf("unsupported first meta type: %s", p.MetaType)}
					return
				}
				first = false
				continue
			}

			switch p.MetaType {
			case PutObject, PartUpload:
				if h == nil {
					out <- ResponsePayload{MetaType: Error, Err: fmt.Errorf("missing upload handler")}
					return
				}

				h.Put(p.Data)
				if !forward(<-hResp) {
					return
				}
			case CompletePutObject, CompletePartUpload:
				if h == nil {
					out <- ResponsePayload{MetaType: Error, Err: fmt.Errorf("missing upload handler")}
					return
				}
				h.Complete(ctx)
				forward(<-hResp)
				return
			case Error:
				if h != nil {
					h.TimeOut()
					forward(<-hResp)
				} else {
					out <- ResponsePayload{MetaType: Error, Err: fmt.Errorf("channel timeout")}
				}
				return
			default:
				out <- ResponsePayload{MetaType: Error, Err: fmt.Errorf("unexpected meta type: %s", p.MetaType)}
				return
			}
		}
	}()

	return out
}
