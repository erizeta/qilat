package h1

import (
	"bufio"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/erizeta/qilat/internal/exchange"
)

func newBody(t *testing.T, raw string, info reqInfo, limit int64) *body {
	t.Helper()
	c := &Conn{cfg: &Config{MaxHeaderBytes: 1 << 10}}
	c.br = bufio.NewReaderSize(strings.NewReader(raw), 16)
	c.lr.br = c.br
	c.body.reset(c, &info, limit)
	return &c.body
}

func TestBodyContentLength(t *testing.T) {
	b := newBody(t, "hello world", reqInfo{contentLength: 5}, -1)
	got, err := io.ReadAll(b)
	if err != nil || string(got) != "hello" {
		t.Fatalf("ReadAll = %q, %v", got, err)
	}
	if n, err := b.Read(make([]byte, 1)); n != 0 || err != io.EOF {
		t.Fatalf("Read after EOF = %d, %v", n, err)
	}
	if n, err := b.Read(nil); n != 0 || err != io.EOF {
		t.Fatalf("empty Read after EOF = %d, %v", n, err)
	}
}

func TestBodyContentLengthTruncated(t *testing.T) {
	b := newBody(t, "abc", reqInfo{contentLength: 10}, -1)
	if _, err := io.ReadAll(b); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("err = %v", err)
	}
	if _, err := b.Read(make([]byte, 1)); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("error not sticky: %v", err)
	}
}

func TestBodyNone(t *testing.T) {
	b := newBody(t, "ignored", reqInfo{contentLength: -1}, -1)
	if n, err := b.Read(make([]byte, 4)); n != 0 || err != io.EOF {
		t.Fatalf("Read = %d, %v", n, err)
	}
}

func TestBodyChunked(t *testing.T) {
	raw := "5\r\nhello\r\n6;name=value\r\n world\r\n1a\r\n" + strings.Repeat("x", 26) + "\r\n0\r\nX-Trailer: t\r\n\r\nNEXT"
	b := newBody(t, raw, reqInfo{chunked: true}, -1)
	got, err := io.ReadAll(b)
	if err != nil || string(got) != "hello world"+strings.Repeat("x", 26) {
		t.Fatalf("ReadAll = %q, %v", got, err)
	}
	rest, _ := io.ReadAll(b.c.br)
	if string(rest) != "NEXT" {
		t.Fatalf("bytes after body = %q", rest)
	}
}

func TestBodyChunkedErrors(t *testing.T) {
	tests := []struct {
		name, raw string
		want      error
	}{
		{"bad size", "zz\r\n", errChunkSize},
		{"empty size", "\r\n", errChunkSize},
		{"size with junk", "5 x\r\nhello\r\n0\r\n\r\n", errChunkSize},
		{"size too long", "1000000000000000\r\n", errChunkSize},
		{"long extension", "5;" + strings.Repeat("e", maxChunkLine) + "\r\nhello\r\n0\r\n\r\n", errChunkSize},
		{"control in extension", "5;a\x01\r\nhello\r\n0\r\n\r\n", errChunkSize},
		{"bare LF size", "5\nhello\r\n0\r\n\r\n", errChunkSize},
		{"missing CRLF", "5\r\nhelloXX0\r\n\r\n", errChunkCRLF},
		{"truncated data", "5\r\nhel", io.ErrUnexpectedEOF},
		{"truncated CRLF", "5\r\nhello", io.ErrUnexpectedEOF},
		{"truncated size", "5", io.ErrUnexpectedEOF},
		{"bad trailer", "0\r\nbad trailer\r\n\r\n", errTrailer},
		{"folded trailer", "0\r\n continued\r\n\r\n", errTrailer},
		{"huge trailer", "0\r\nX: " + strings.Repeat("t", 2000) + "\r\n\r\n", errTrailer},
		{"truncated trailer", "0\r\nX: t", io.ErrUnexpectedEOF},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := newBody(t, tt.raw, reqInfo{chunked: true}, -1)
			if _, err := io.ReadAll(b); !errors.Is(err, tt.want) {
				t.Fatalf("err = %v, want %v", err, tt.want)
			}
		})
	}
}

func TestBodyChunkedLimit(t *testing.T) {
	b := newBody(t, "5\r\nhello\r\n5\r\nworld\r\n0\r\n\r\n", reqInfo{chunked: true}, 8)
	got, err := io.ReadAll(b)
	if !errors.Is(err, exchange.ErrBodyTooLarge) || string(got) != "hello" {
		t.Fatalf("ReadAll = %q, %v", got, err)
	}
}

func TestBodyDrain(t *testing.T) {
	if b := newBody(t, "0123456789", reqInfo{contentLength: 10}, -1); !b.drain(maxDrain) || !b.eof {
		t.Fatal("small body not drained")
	}
	if b := newBody(t, "", reqInfo{contentLength: maxDrain + 1}, -1); b.drain(maxDrain) {
		t.Fatal("body larger than the drain limit reported drained")
	}
	chunked := strings.Repeat("10\r\n"+strings.Repeat("c", 16)+"\r\n", 10) + "0\r\n\r\n"
	if b := newBody(t, chunked, reqInfo{chunked: true}, -1); !b.drain(maxDrain) {
		t.Fatal("chunked body not drained")
	}
	if b := newBody(t, chunked, reqInfo{chunked: true}, -1); b.drain(32) {
		t.Fatal("chunked body over the drain limit reported drained")
	}
	if b := newBody(t, "zz\r\n", reqInfo{chunked: true}, -1); b.drain(maxDrain) {
		t.Fatal("malformed body reported drained")
	}
	b := newBody(t, "abc", reqInfo{contentLength: 10}, -1)
	_, _ = io.ReadAll(b)
	if b.drain(maxDrain) {
		t.Fatal("failed body reported drained")
	}
	if b := newBody(t, "", reqInfo{contentLength: -1}, -1); !b.drain(maxDrain) {
		t.Fatal("empty body not drained")
	}
}

func TestBodyClose(t *testing.T) {
	b := newBody(t, "hello", reqInfo{contentLength: 5}, -1)
	b.close()
	if _, err := b.Read(make([]byte, 1)); !errors.Is(err, errBodyClosed) {
		t.Fatalf("Read after close = %v", err)
	}
}

func FuzzChunkedBody(f *testing.F) {
	for _, s := range []string{"5\r\nhello\r\n0\r\n\r\n", "1;a=b\r\nx\r\n0\r\nT: v\r\n\r\n", "zz\r\n", "0\r\n\r\n"} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, raw string) {
		b := newBody(t, raw, reqInfo{chunked: true}, 1<<20)
		got, err := io.ReadAll(b)
		if err == nil && int64(len(got)) != b.total {
			t.Fatalf("read %d bytes, counted %d", len(got), b.total)
		}
	})
}
