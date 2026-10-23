package http2

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"

	http "github.com/FastTLS/fhttp"
	tls "github.com/refraction-networking/utls"
)

func TestPooledDialCancellationIsolation(t *testing.T) {
	started, stopped := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	retryError := errors.New("new dial after canceled waiters")
	tr := &Transport{DialTLSContext: func(ctx context.Context, _, _ string, _ *tls.Config) (net.Conn, error) {
		if calls.Add(1) != 1 {
			return nil, retryError
		}
		close(started)
		<-ctx.Done()
		close(stopped)
		return nil, ctx.Err()
	}}
	p := &clientConnPool{t: tr}
	ctx1, cancel1 := context.WithCancel(context.Background())
	defer cancel1()
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	request := func(ctx context.Context) *http.Request {
		r, _ := http.NewRequestWithContext(ctx, "GET", "https://fixture.invalid", nil)
		return r
	}
	first, second := make(chan error, 1), make(chan error, 1)
	go func() { _, err := p.GetClientConn(request(ctx1), "fixture.invalid:443"); first <- err }()
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("dial did not start")
	}
	go func() { _, err := p.GetClientConn(request(ctx2), "fixture.invalid:443"); second <- err }()
	deadline := time.Now().Add(3 * time.Second)
	for {
		p.mu.Lock()
		call := p.dialing["fixture.invalid:443"]
		joined := call != nil && call.waiters == 2
		p.mu.Unlock()
		if joined {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("second request did not join dial")
		}
		time.Sleep(time.Millisecond)
	}
	cancel1()
	select {
	case err := <-first:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("first waiter retained")
	}
	select {
	case <-stopped:
		t.Fatal("one waiter canceled the other request's dial")
	default:
	}
	cancel2()
	select {
	case err := <-second:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("second waiter retained")
	}
	select {
	case <-stopped:
	case <-time.After(3 * time.Second):
		t.Fatal("dial retained after last waiter canceled")
	}
	_, err := p.GetClientConn(request(context.Background()), "fixture.invalid:443")
	if !errors.Is(err, retryError) || calls.Load() != 2 {
		t.Fatalf("retry: calls=%d err=%v", calls.Load(), err)
	}
}
