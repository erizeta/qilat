package h1

import (
	"bufio"
	"bytes"
	"testing"
)

func TestCanonicalNameMatchesGeneric(t *testing.T) {
	for _, name := range []string{
		"content-type", "content-length", "content-encoding", "date", "server", "alt-svc", "vary",
		"cache-control", "set-cookie", "location", "etag", "last-modified",
	} {
		if got, want := canonicalName([]byte(name)), string(appendCanonical(nil, []byte(name))); got != want {
			t.Errorf("canonicalName(%q) = %q, want %q", name, got, want)
		}
	}
	if s := canonicalName([]byte("x-custom")); s != "" {
		t.Errorf("canonicalName(x-custom) = %q", s)
	}
}

func TestWriteField(t *testing.T) {
	for _, size := range []int{16, 4096} {
		var out bytes.Buffer
		bw := bufio.NewWriterSize(&out, size)
		writeField(bw, []byte("content-type"), []byte("text/plain"))
		writeField(bw, []byte("x-request-id-with-a-long-name"), []byte("1"))
		writeField(bw, []byte("www-authenticate"), []byte("Basic"))
		_ = bw.Flush()
		want := "Content-Type: text/plain\r\nX-Request-Id-With-A-Long-Name: 1\r\nWww-Authenticate: Basic\r\n"
		if out.String() != want {
			t.Errorf("buffer %d: got %q, want %q", size, out.String(), want)
		}
	}
}

func TestWriteFieldAllocs(t *testing.T) {
	bw := bufio.NewWriterSize(discard{}, 4096)
	name, value := []byte("x-custom-field"), []byte("value")
	if n := testing.AllocsPerRun(100, func() {
		writeField(bw, name, value)
		_ = bw.Flush()
	}); n != 0 {
		t.Fatalf("allocs = %v", n)
	}
}

type discard struct{}

func (discard) Write(p []byte) (int, error) { return len(p), nil }
