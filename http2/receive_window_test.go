package http2

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"net"
	"testing"
)

func TestReceiveWindowMatchesWire(t *testing.T) {
	for _, window := range []uint32{0, 1024, 65535, 6291456} {
		t.Run(fmt.Sprint(window), func(t *testing.T) {
			client, server := net.Pipe()
			settings := make(chan []Setting, 1)
			done := make(chan struct{})
			go func() {
				defer close(done)
				if _, err := io.ReadFull(server, make([]byte, len(clientPreface))); err != nil {
					return
				}
				fr := NewFramer(io.Discard, server)
				for {
					frame, err := fr.ReadFrame()
					if err != nil {
						return
					}
					if s, ok := frame.(*SettingsFrame); ok && !s.IsAck() {
						var got []Setting
						s.ForeachSetting(func(s Setting) error { got = append(got, s); return nil })
						settings <- got
					}
				}
			}()
			defer func() { client.Close(); server.Close(); <-done }()
			transport := &Transport{HTTP2Settings: &HTTP2Settings{Explicit: true, Settings: []Setting{{ID: SettingInitialWindowSize, Val: window}}, ConnectionFlow: 1000000}}
			cc, err := transport.NewClientConn(client)
			if err != nil {
				t.Fatal(err)
			}
			defer cc.Close()
			wire := <-settings
			if len(wire) != 1 || wire[0].Val != window {
				t.Fatalf("wire settings changed: %v", wire)
			}
			cc.mu.Lock()
			cs := cc.newStreamWithID(1, false)
			stream, connection := cs.inflow.n, cc.inflow.n
			cc.mu.Unlock()
			if stream != int32(window) || connection != 65535+1000000 {
				t.Fatalf("wire/local mismatch: stream=%d conn=%d", stream, connection)
			}
		})
	}
}

func TestReceiveWindowRefreshSmallAndLarge(t *testing.T) {
	for _, window := range []uint32{1024, 6291456} {
		t.Run(fmt.Sprint(window), func(t *testing.T) {
			var wire bytes.Buffer
			cc := &ClientConn{receiveStreamWindow: window, receiveConnWindow: 65535, bw: bufio.NewWriter(&wire)}
			cc.fr = NewFramer(cc.bw, nil)
			cc.inflow.add(65535)
			cs := &clientStream{cc: cc, ID: 1, bytesRemain: -1, bufPipe: pipe{b: bytes.NewBuffer(make([]byte, window))}}
			cs.inflow.add(int32(window))
			cs.inflow.setConnFlow(&cc.inflow)
			// Simulate a valid small DATA frame followed by application consumption.
			n := min(int(window), 8192)
			cs.inflow.take(int32(n))
			cs.bufPipe.b = bytes.NewBuffer(make([]byte, n))
			if got, err := (transportResponseBody{cs}).Read(make([]byte, n)); got != n || err != nil {
				t.Fatalf("read: %d %v", got, err)
			}
			fr := NewFramer(nil, &wire)
			frame, err := fr.ReadFrame()
			if err != nil {
				t.Fatal(err)
			}
			update, ok := frame.(*WindowUpdateFrame)
			if !ok || update.StreamID != 1 || update.Increment != uint32(n) {
				t.Fatalf("wrong stream refund: %#v", frame)
			}
			if cs.inflow.n != int32(window) {
				t.Fatalf("receive target drifted: %d", cs.inflow.n)
			}
		})
	}
}
