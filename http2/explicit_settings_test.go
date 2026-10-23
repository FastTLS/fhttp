package http2

import (
	"bytes"
	"encoding/hex"
	"testing"
)

func TestExplicitZeroDoesNotSelectNavigatorDefaults(t *testing.T) {
	var b bytes.Buffer
	fr := NewFramer(&b, nil)
	flow, next, err := fr.AutoWriteFrames(&Transport{Navigator: "firefox", HTTP2Settings: &HTTP2Settings{Explicit: true, Settings: []Setting{{ID: SettingInitialWindowSize, Val: 0}}}})
	if err != nil {
		t.Fatal(err)
	}
	// One SETTINGS frame, no WINDOW_UPDATE or Firefox PRIORITY frames.
	want, _ := hex.DecodeString("000006040000000000000400000000")
	if !bytes.Equal(b.Bytes(), want) || flow != initialWindowSize || next != 1 {
		t.Fatalf("explicit zero replaced: %x flow=%d next=%d", b.Bytes(), flow, next)
	}
}
func TestExplicitEmptySettings(t *testing.T) {
	var b bytes.Buffer
	flow, next, err := NewFramer(&b, nil).AutoWriteFrames(&Transport{HTTP2Settings: &HTTP2Settings{Explicit: true}})
	want, _ := hex.DecodeString("000000040000000000")
	if err != nil || !bytes.Equal(b.Bytes(), want) || flow != 65535 || next != 1 {
		t.Fatalf("empty settings changed: %x %d %d %v", b.Bytes(), flow, next, err)
	}
}
