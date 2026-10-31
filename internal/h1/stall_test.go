package h1

import (
	"io"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// readFromConn records what the connection's ReadFrom receives, as sendfile would see it.
type readFromConn struct {
	net.Conn
	chunks    []int64
	files     int
	deadlines int
}

func (c *readFromConn) RemoteAddr() net.Addr             { return &net.TCPAddr{} }
func (c *readFromConn) SetWriteDeadline(time.Time) error { c.deadlines++; return nil }

func (c *readFromConn) ReadFrom(r io.Reader) (int64, error) {
	lr, ok := r.(*io.LimitedReader)
	if ok {
		c.chunks = append(c.chunks, lr.N)
		if _, ok := lr.R.(*os.File); ok {
			c.files++
		}
	}
	return io.Copy(io.Discard, r)
}

func TestStallReaderFromKeepsSendfile(t *testing.T) {
	const size = 3*stallChunk + 100
	path := filepath.Join(t.TempDir(), "f")
	if err := os.WriteFile(path, make([]byte, size+50), 0o600); err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()

	nc := &readFromConn{}
	c := NewConn(nc, &Config{WriteStallTimeout: time.Second})
	w, ok := c.socketWriter().(io.ReaderFrom)
	if !ok {
		t.Fatalf("socket writer %T does not keep ReadFrom", c.socketWriter())
	}
	lr := &io.LimitedReader{R: f, N: size}
	n, err := w.ReadFrom(lr)
	if err != nil || n != size {
		t.Fatalf("ReadFrom = %d, %v; want %d", n, err, size)
	}
	if lr.N != 0 {
		t.Errorf("LimitedReader left with N = %d, want 0", lr.N)
	}
	if nc.files != len(nc.chunks) || len(nc.chunks) != 4 {
		t.Errorf("ReadFrom calls %v, %d with the file; want 4 chunks of the file", nc.chunks, nc.files)
	}
	for _, chunk := range nc.chunks {
		if chunk > stallChunk {
			t.Errorf("chunk of %d bytes, over %d", chunk, stallChunk)
		}
	}
	// Chunks that follow each other quickly keep the deadline set for the first one.
	if nc.deadlines != 1 {
		t.Errorf("%d write deadlines for %d quick chunks, want 1", nc.deadlines, len(nc.chunks))
	}
}
