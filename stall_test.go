package qilat

import (
	"bufio"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

const testStall = 150 * time.Millisecond

// stallResult is what a handler saw when a read or write stopped making progress.
type stallResult struct {
	err      error
	canceled bool
	after    time.Duration
}

func (r stallResult) check(t *testing.T) {
	t.Helper()
	if !errors.Is(r.err, os.ErrDeadlineExceeded) {
		t.Fatalf("error %v, want one wrapping os.ErrDeadlineExceeded", r.err)
	}
	if !r.canceled {
		t.Error("request context not canceled")
	}
	if r.after < testStall || r.after > 20*testStall {
		t.Errorf("failed after %v with a %v stall timeout", r.after, testStall)
	}
}

// slowReader lets a client consume a response slowly but steadily: at most n bytes per Read, after a pause.
type slowReader struct {
	r     io.Reader
	n     int
	pause time.Duration
}

func (s slowReader) Read(p []byte) (int, error) {
	time.Sleep(s.pause)
	return s.r.Read(p[:min(len(p), s.n)])
}

func TestServerStallDefaults(t *testing.T) {
	s := &Server{}
	cfg := s.h1Config(&exchangeConfig{}, 0, 0)
	if cfg.ReadStallTimeout != DefaultStallTimeout || cfg.WriteStallTimeout != DefaultStallTimeout {
		t.Errorf("zero values resolve to %v, %v", cfg.ReadStallTimeout, cfg.WriteStallTimeout)
	}
	if h2 := s.h2Config(&exchangeConfig{}, 0, 0); h2.ReadStallTimeout != DefaultStallTimeout || h2.WriteStallTimeout != DefaultStallTimeout {
		t.Errorf("HTTP/2 zero values resolve to %v, %v", h2.ReadStallTimeout, h2.WriteStallTimeout)
	}
	s.ReadStallTimeout, s.WriteStallTimeout = -1, -1
	if cfg := s.h1Config(&exchangeConfig{}, 0, 0); cfg.ReadStallTimeout != 0 || cfg.WriteStallTimeout != 0 {
		t.Errorf("negative values resolve to %v, %v, want disabled", cfg.ReadStallTimeout, cfg.WriteStallTimeout)
	}
}

func TestH1ReadStall(t *testing.T) {
	got := make(chan stallResult, 1)
	ts := startServer(t, func(w ResponseWriter, r *Request) {
		start := time.Now()
		_, err := io.ReadAll(r.Body)
		got <- stallResult{err, r.Context().Err() != nil, time.Since(start)}
	}, func(s *Server) { s.ReadStallTimeout = testStall })
	c := dialRaw(t, ts.addr)
	c.send("POST / HTTP/1.1\r\nHost: h\r\nContent-Length: 10\r\n\r\nab")
	(<-got).check(t)
	_ = c.response("POST")
	if !c.closed() {
		t.Error("connection kept open after a stalled body")
	}
}

func TestH1SlowUploadIsNotAStall(t *testing.T) {
	ts := startServer(t, func(w ResponseWriter, r *Request) {
		b, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("reading a slow upload: %v", err)
		}
		_, _ = w.WriteString(strconv.Itoa(len(b)))
	}, func(s *Server) { s.ReadStallTimeout = testStall })
	c := dialRaw(t, ts.addr)
	c.send("POST / HTTP/1.1\r\nHost: h\r\nContent-Length: 10\r\n\r\n")
	for range 10 { // about four stall timeouts in total, but never one without progress
		time.Sleep(testStall / 3)
		c.send("x")
	}
	if body := c.body(c.response("POST")); body != "10" {
		t.Fatalf("body %q", body)
	}
}

func TestH1WriteStall(t *testing.T) {
	got := make(chan stallResult, 1)
	ts := startServer(t, func(w ResponseWriter, r *Request) {
		start := time.Now()
		chunk := make([]byte, 1<<20)
		var err error
		for i := 0; i < 256 && err == nil; i++ {
			_, err = w.Write(chunk)
		}
		got <- stallResult{err, r.Context().Err() != nil, time.Since(start)}
	}, func(s *Server) { s.WriteStallTimeout = testStall })
	c := dialRaw(t, ts.addr)
	c.send("GET / HTTP/1.1\r\nHost: h\r\n\r\n") // and never read the response
	res := <-got
	// The socket buffers absorb some data before the first write blocks, which the stall does not count.
	res.after = max(res.after, testStall)
	res.check(t)
}

// TestH1SlowReaderIsNotAStall sends large responses to a client that reads steadily but slower than the stall
// timeout would allow for the whole transfer: one large Write, and a file sent with sendfile.
func TestH1SlowReaderIsNotAStall(t *testing.T) {
	const size = 4 << 20
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "big"), make([]byte, size), 0o600); err != nil {
		t.Fatal(err)
	}
	files := FileServer(os.DirFS(dir))
	ts := startServer(t, func(w ResponseWriter, r *Request) {
		if strings.HasPrefix(string(r.Path()), "/big") {
			files.ServeHTTP(w, r)
			return
		}
		w.Header().Set("Content-Length", strconv.Itoa(size))
		if _, err := w.Write(make([]byte, size)); err != nil {
			t.Errorf("write to a slow reader: %v", err)
		}
	}, func(s *Server) { s.WriteStallTimeout = testStall })
	for _, path := range []string{"/write", "/big"} {
		t.Run(path, func(t *testing.T) {
			nc, err := net.Dial("tcp", ts.addr)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = nc.Close() }()
			if tcp, ok := nc.(*net.TCPConn); ok {
				_ = tcp.SetReadBuffer(64 << 10) // keep the kernel from buffering the whole response
			}
			_, _ = io.WriteString(nc, "GET "+path+" HTTP/1.1\r\nHost: h\r\n\r\n")
			// About 3 MB/s takes over a second, several stall timeouts, but each 64 KiB arrives in time.
			resp, err := http.ReadResponse(bufio.NewReaderSize(slowReader{nc, 32 << 10, 10 * time.Millisecond}, 64<<10), nil)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = resp.Body.Close() }()
			n, err := io.Copy(io.Discard, resp.Body)
			if err != nil || n != size {
				t.Fatalf("read %d of %d bytes: %v", n, size, err)
			}
		})
	}
}
