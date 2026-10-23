package http2

import (
	"context"
	"errors"
	"io"
	stdhttp "net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	http "github.com/FastTLS/fhttp"
	tls "github.com/refraction-networking/utls"
)

type blockedUpload struct {
	*io.PipeReader
	entered chan struct{}
	once    sync.Once
	closes  atomic.Int32
}

func (b *blockedUpload) Read(p []byte) (int, error) {
	b.once.Do(func() { close(b.entered) })
	return b.PipeReader.Read(p)
}
func (b *blockedUpload) Close() error { b.closes.Add(1); return b.PipeReader.Close() }

func TestStreamingUploadBlockedProducerStops(t *testing.T) {
	for _, cancelRequest := range []bool{false, true} {
		t.Run(map[bool]string{false: "early_response", true: "cancel"}[cancelRequest], func(t *testing.T) {
			reader, writer := io.Pipe()
			defer writer.Close()
			body := &blockedUpload{PipeReader: reader, entered: make(chan struct{})}
			server := httptest.NewUnstartedServer(stdhttp.HandlerFunc(func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
				select {
				case <-body.entered:
				case <-r.Context().Done():
					return
				}
				if cancelRequest {
					<-r.Context().Done()
					return
				}
				w.WriteHeader(413)
				io.WriteString(w, "denied")
			}))
			server.EnableHTTP2 = true
			server.StartTLS()
			defer server.Close()
			conn, err := tls.Dial("tcp", server.Listener.Addr().String(), &tls.Config{InsecureSkipVerify: true, NextProtos: []string{"h2"}})
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			tr := &Transport{}
			cc, err := tr.NewClientConn(conn)
			if err != nil {
				t.Fatal(err)
			}
			defer cc.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			req, _ := http.NewRequestWithContext(ctx, "POST", server.URL, body)
			done := make(chan error, 1)
			go func() {
				response, err := cc.RoundTrip(req)
				if err == nil {
					data, readErr := io.ReadAll(response.Body)
					response.Body.Close()
					if readErr != nil {
						err = readErr
					} else if response.StatusCode != 413 || string(data) != "denied" {
						err = errors.New("early response lost")
					}
				}
				done <- err
			}()
			if cancelRequest {
				select {
				case <-body.entered:
					cancel()
				case <-ctx.Done():
					t.Fatal("body wasn't read")
				}
			}
			select {
			case err := <-done:
				if cancelRequest {
					if !errors.Is(err, context.Canceled) {
						t.Fatal(err)
					}
				} else if err != nil {
					t.Fatal(err)
				}
			case <-time.After(2 * time.Second):
				cancel()
				t.Fatal("blocked body prevented early response/cancellation")
			}
			if body.closes.Load() != 1 {
				t.Fatalf("body closed %d times", body.closes.Load())
			}
		})
	}
}
