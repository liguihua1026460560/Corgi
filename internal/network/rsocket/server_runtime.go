package rsocket

import (
	"context"
	"fmt"
	"log"
	"sync"

	gorsocket "github.com/rsocket/rsocket-go"
	rspayload "github.com/rsocket/rsocket-go/payload"
	"github.com/rsocket/rsocket-go/rx/flux"
	"github.com/rsocket/rsocket-go/rx/mono"
)

// StartRSocketServer starts the backend RSocket server.
//
// Java uses frameDecoder(MsPayloadDecoder.DEFAULT), a custom event loop, and a
// TcpDuplexConnection reflection hook. rsocket-go already decodes payloads into
// owned byte slices, and Go's runtime schedules handler goroutines directly, so
// this bootstrap only needs the acceptor and TCP transport.
func StartRSocketServer(ctx context.Context, addr string, erasure *ErasureServer) error {
	if ctx == nil {
		return fmt.Errorf("nil context")
	}
	if addr == "" {
		return fmt.Errorf("empty rsocket address")
	}
	if erasure == nil {
		return fmt.Errorf("nil erasure server")
	}

	return gorsocket.Receive().
		OnStart(func() {
			log.Printf("rsocket server bind addr: %s", addr)
		}).
		Acceptor(func(ctx context.Context, _ rspayload.SetupPayload, _ gorsocket.CloseableRSocket) (gorsocket.RSocket, error) {
			return newResponder(erasure), nil
		}).
		Transport(gorsocket.TCPServer().SetAddr(addr).Build()).
		Serve(ctx)
}

func newResponder(erasure *ErasureServer) gorsocket.RSocket {
	return gorsocket.NewAbstractSocket(
		gorsocket.RequestResponse(func(p rspayload.Payload) mono.Mono {
			meta, _ := p.MetadataUTF8()
			if meta == "PING" {
				return mono.Just(EncodePayload(ResponsePayload{MetaType: Success}))
			}
			return mono.Just(EncodePayload(ResponsePayload{
				MetaType: Error,
				Err:      fmt.Errorf("unsupported request-response meta type: %s", meta),
			}))
		}),
		gorsocket.RequestChannel(func(payloads flux.Flux) flux.Flux {
			return requestChannelFlux(erasure, payloads)
		}),
	)
}

func requestChannelFlux(erasure *ErasureServer, payloads flux.Flux) flux.Flux {
	return flux.Create(func(ctx context.Context, sink flux.Sink) {
		requests := make(chan RequestPayload, 64)
		responses := erasure.RequestChannel(ctx, requests)
		incoming, incomingErr := payloads.ToChan(ctx, 64)

		var finishOnce sync.Once
		finished := make(chan struct{})
		complete := func() {
			finishOnce.Do(func() {
				close(finished)
				sink.Complete()
			})
		}
		emitError := func(err error) {
			finishOnce.Do(func() {
				sink.Next(EncodePayload(ResponsePayload{MetaType: Error, Err: err}))
				close(finished)
				sink.Complete()
			})
		}

		go func() {
			defer close(requests)
			for incoming != nil || incomingErr != nil {
				select {
				case <-ctx.Done():
					return
				case <-finished:
					return
				case p, ok := <-incoming:
					if !ok {
						incoming = nil
						continue
					}
					req, err := DecodePayload(p)
					if err != nil {
						emitError(err)
						return
					}
					select {
					case requests <- req:
					case <-ctx.Done():
						return
					case <-finished:
						return
					}
				case err, ok := <-incomingErr:
					if !ok {
						incomingErr = nil
						continue
					}
					if err != nil {
						emitError(err)
						return
					}
				}
			}
		}()

		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				case <-finished:
					return
				case resp, ok := <-responses:
					if !ok {
						complete()
						return
					}
					sink.Next(EncodePayload(resp))
				}
			}
		}()
	})
}
