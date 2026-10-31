package h1

import (
	"bufio"
	"crypto/tls"
	"errors"
	"io"
	"log/slog"
	"net"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/erizeta/qilat/internal/exchange"
)

// Config holds the server settings for HTTP/1.1 connections. Zero durations and negative sizes mean no limit;
// the core resolves its defaults before building a Config.
type Config struct {
	// Acquire returns a fresh exchange for each request.
	Acquire            func() exchange.Exchange
	ReadHeaderTimeout  time.Duration
	ReadTimeout        time.Duration
	WriteTimeout       time.Duration
	ReadStallTimeout   time.Duration
	WriteStallTimeout  time.Duration
	IdleTimeout        time.Duration
	MaxHeaderBytes     int
	MaxRequestBodySize int64
	// ReduceMemoryUsage returns the read buffer to a pool while idle, at the cost of one extra read per request.
	ReduceMemoryUsage bool
	ShuttingDown      *atomic.Bool
	Logger            *slog.Logger
}

const (
	bufferSize = 4 << 10
	// maxDrain is how much unread request body is discarded to keep a connection alive, as in net/http.
	maxDrain = 256 << 10
)

const (
	stateActive int32 = iota
	stateIdle
	stateHijacked
	stateClosed
)

var (
	readerPool sync.Pool
	writerPool sync.Pool
)

// aLongTimeAgo is a deadline in the past that unblocks a pending read.
var aLongTimeAgo = time.Unix(1, 0)

// Conn serves HTTP/1.1 requests on one connection, one at a time.
type Conn struct {
	nc       net.Conn
	cfg      *Config
	remote   net.Addr
	tlsState *tls.ConnectionState
	state    atomic.Int32

	cr   connReader
	br   *bufio.Reader
	bw   *bufio.Writer
	sw   stallWriter     // the bufio.Writer's destination when WriteStallTimeout is set
	srf  stallReaderFrom // the same, for connections that implement io.ReaderFrom (sendfile)
	lr   lineReader
	body body
	sink sink

	// Per-request state, reset by serveRequest.
	x             exchange.Exchange
	info          reqInfo
	headerWritten bool
	continueSent  bool
	chunkedResp   bool
	respKeepAlive bool
	hijacked      bool
	readingBody   bool      // a handler is reading the body, so socket reads are bounded by ReadStallTimeout
	readDeadline  time.Time // from ReadTimeout; zero when unset
	writeDeadline time.Time // from WriteTimeout; zero when unset
	readSet       time.Time // the stall read deadline last set; zero when another deadline replaced it
	writeSet      time.Time // the same for writes
	scratch       [24]byte

	// watchMu guards starting the background read used to notice a client disconnect.
	watchMu     sync.Mutex
	serving     bool
	bodyDone    bool
	watchWanted bool
}

// NewConn prepares nc to be served with cfg.
func NewConn(nc net.Conn, cfg *Config) *Conn {
	c := &Conn{nc: nc, cfg: cfg, remote: nc.RemoteAddr()}
	c.sw.c = c
	c.srf.c = c
	c.cr.c = c
	c.cr.cond.L = &c.cr.mu
	c.sink.c = c
	return c
}

// CloseIfIdle closes the connection if it is waiting for a request, and reports whether it did.
func (c *Conn) CloseIfIdle() bool {
	if c.state.CompareAndSwap(stateIdle, stateClosed) {
		_ = c.nc.Close()
		return true
	}
	return false
}

// Close closes the connection immediately unless it has been hijacked.
func (c *Conn) Close() {
	for {
		s := c.state.Load()
		if s == stateHijacked || s == stateClosed {
			return
		}
		if c.state.CompareAndSwap(s, stateClosed) {
			_ = c.nc.Close()
			return
		}
	}
}

// Serve handles requests until the connection closes, fails, or is hijacked.
func (c *Conn) Serve() {
	defer c.finish()
	if tc, ok := c.nc.(*tls.Conn); ok && !c.handshake(tc) {
		return
	}
	if !c.cfg.ReduceMemoryUsage {
		c.br = getReader(&c.cr)
	}
	c.lr.br = c.br
	for first := true; c.waitForRequest(first); first = false {
		if !c.serveRequest() {
			return
		}
	}
}

func (c *Conn) handshake(tc *tls.Conn) bool {
	if d := c.cfg.ReadHeaderTimeout; d > 0 {
		_ = tc.SetDeadline(time.Now().Add(d))
	}
	if err := tc.Handshake(); err != nil {
		if c.cfg.Logger != nil {
			c.cfg.Logger.Debug("qilat: TLS handshake failed", "remote", c.remote, "error", err)
		}
		return false
	}
	_ = tc.SetDeadline(time.Time{})
	st := tc.ConnectionState()
	c.tlsState = &st
	return true
}

// waitForRequest blocks until the first byte of the next request arrives. The connection is idle while it waits,
// so Shutdown may close it; the state transition back to active decides which side wins.
func (c *Conn) waitForRequest(first bool) bool {
	// A pipelined request already in the buffer is served at once, so its response is batched with this one.
	if c.bw != nil && (c.br == nil || c.br.Buffered() == 0) {
		err := c.bw.Flush()
		putWriter(c.bw)
		c.bw = nil
		if err != nil {
			return false
		}
	}
	if !c.state.CompareAndSwap(stateActive, stateIdle) || c.cfg.ShuttingDown.Load() {
		return false
	}
	timeout := c.cfg.IdleTimeout
	if first {
		timeout = c.cfg.ReadHeaderTimeout
	}
	var deadline time.Time
	if timeout > 0 {
		deadline = time.Now().Add(timeout)
	}
	if c.nc.SetReadDeadline(deadline) != nil {
		return false
	}
	var err error
	if c.cfg.ReduceMemoryUsage {
		if c.br != nil && c.br.Buffered() == 0 {
			putReader(c.br)
			c.br = nil
		}
		if c.br == nil {
			if err = c.cr.readAhead(); err == nil {
				c.br = getReader(&c.cr)
				c.lr.br = c.br
			}
		}
	} else {
		_, err = c.br.Peek(1)
	}
	return err == nil && c.state.CompareAndSwap(stateIdle, stateActive)
}

// serveRequest reads, serves, and completes one request. It reports whether the connection can serve another.
func (c *Conn) serveRequest() bool {
	start := time.Now()
	if d := c.cfg.ReadHeaderTimeout; d > 0 {
		_ = c.nc.SetReadDeadline(start.Add(d))
	}
	// Error responses to a request that failed to parse must not inherit the previous request's deadlines.
	// The read deadline was just replaced; the write deadline is replaced only with WriteTimeout, so without one
	// the stall deadline set for an earlier response is still in place and still known.
	c.readDeadline, c.writeDeadline = time.Time{}, time.Time{}
	c.readSet = time.Time{}
	if c.cfg.WriteTimeout > 0 {
		c.writeSet = time.Time{}
	}
	c.lr.budget = c.cfg.MaxHeaderBytes
	c.headerWritten, c.continueSent, c.chunkedResp, c.respKeepAlive = false, false, false, false

	x := c.cfg.Acquire()
	info, err := parseRequest(&c.lr, x)
	if err == nil && c.cfg.MaxRequestBodySize >= 0 && info.contentLength > c.cfg.MaxRequestBodySize {
		err = errBodyTooLarge
	}
	if err != nil {
		x.Release()
		c.writeError(err)
		return false
	}
	c.info = info

	if d := c.cfg.ReadTimeout; d > 0 {
		c.readDeadline = start.Add(d)
	}
	// The read deadline is replaced here anyway, so a body's first stall deadline costs no extra timer update.
	readDeadline := c.readDeadline
	if d := c.cfg.ReadStallTimeout; d > 0 && (info.chunked || info.contentLength > 0) {
		c.readSet = earliest(time.Now().Add(d), c.readDeadline)
		readDeadline = c.readSet
	}
	_ = c.nc.SetReadDeadline(readDeadline)
	if d := c.cfg.WriteTimeout; d > 0 {
		c.writeDeadline = time.Now().Add(d)
		_ = c.nc.SetWriteDeadline(c.writeDeadline)
	}

	c.body.reset(c, &info, c.cfg.MaxRequestBodySize)
	if c.body.mode == bodyNone {
		x.SetBody(nil, 0)
	} else {
		x.SetBody(&c.body, info.contentLength)
	}
	x.SetConn(c.remote, c.tlsState)
	x.SetDisconnectWatcher(c)
	c.x = x
	c.watchMu.Lock()
	c.serving, c.bodyDone, c.watchWanted = true, c.body.eof, false
	c.watchMu.Unlock()

	serveErr := x.Serve(&c.sink)

	c.watchMu.Lock()
	c.serving = false
	c.watchMu.Unlock()
	c.cr.abortPendingRead()
	if c.hijacked {
		c.x = nil
		x.Release()
		return false
	}
	keepAlive := serveErr == nil && c.respKeepAlive
	if keepAlive && !c.body.eof {
		// Without a 100 Continue the client may never send the body, so it cannot be drained.
		keepAlive = (!info.expectContinue || c.continueSent) && c.body.drain(maxDrain)
	}
	c.body.close()
	c.x = nil
	x.Release()
	return keepAlive
}

// beforeBodyRead sends 100 Continue the first time a handler reads a body the client is holding back.
func (c *Conn) beforeBodyRead() {
	if !c.info.expectContinue || c.continueSent || c.headerWritten {
		return
	}
	c.continueSent = true
	bw := c.writer()
	_, _ = bw.WriteString("HTTP/1.1 100 Continue\r\n\r\n")
	_ = bw.Flush()
}

func (c *Conn) onBodyEOF() {
	c.watchMu.Lock()
	c.bodyDone = true
	c.maybeWatchLocked()
	c.watchMu.Unlock()
}

// WatchDisconnect implements exchange.DisconnectWatcher.
func (c *Conn) WatchDisconnect() {
	c.watchMu.Lock()
	c.watchWanted = true
	c.maybeWatchLocked()
	c.watchMu.Unlock()
}

// maybeWatchLocked starts a background read once the body is consumed, as net/http does. Pipelined requests
// already in the buffer make disconnect detection impossible, so it is skipped then.
func (c *Conn) maybeWatchLocked() {
	if c.serving && c.bodyDone && c.watchWanted && !c.hijacked && c.br.Buffered() == 0 {
		c.watchWanted = false
		c.cr.startBackgroundRead()
	}
}

func (c *Conn) writer() *bufio.Writer {
	if c.bw == nil {
		c.bw = getWriter(c.socketWriter())
	}
	return c.bw
}

// writeError answers a request that could not be parsed and closes the connection afterwards.
func (c *Conn) writeError(err error) {
	var pe *parseError
	if !errors.As(err, &pe) {
		return // I/O errors and timeouts: the client is gone or too slow, nothing useful to send
	}
	_ = c.nc.SetWriteDeadline(time.Now().Add(5 * time.Second))
	text := statusText[pe.status]
	bw := c.writer()
	writeStatusLine(bw, pe.status)
	_, _ = bw.WriteString("Content-Type: text/plain; charset=utf-8\r\nConnection: close\r\nContent-Length: ")
	_, _ = bw.WriteString(strconv.Itoa(len(text) + 4))
	_, _ = bw.WriteString("\r\n\r\n")
	_, _ = bw.WriteString(strconv.Itoa(pe.status) + " " + text)
	_ = bw.Flush()
}

func (c *Conn) finish() {
	if c.hijacked {
		return
	}
	if c.bw != nil {
		_ = c.bw.Flush()
		putWriter(c.bw)
		c.bw = nil
	}
	if c.br != nil {
		putReader(c.br)
		c.br = nil
	}
	c.state.Store(stateClosed)
	_ = c.nc.Close()
}

// connReader sits between the connection and its bufio.Reader so a byte read in the background,
// or ahead while idle, is delivered to the next Read.
type connReader struct {
	c       *Conn
	mu      sync.Mutex
	cond    sync.Cond
	inRead  bool
	aborted bool
	hasByte bool
	b       [1]byte
}

func (cr *connReader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	cr.mu.Lock()
	if cr.hasByte {
		p[0] = cr.b[0]
		cr.hasByte = false
		cr.mu.Unlock()
		return 1, nil
	}
	cr.mu.Unlock()
	c := cr.c
	if c.readingBody && c.cfg.ReadStallTimeout > 0 {
		c.readSet = c.moveDeadline(c.readSet, c.cfg.ReadStallTimeout, c.readDeadline, c.nc.SetReadDeadline)
	}
	return c.nc.Read(p)
}

// readAhead blocks until one byte arrives and keeps it for the next Read.
func (cr *connReader) readAhead() error {
	n, err := cr.c.nc.Read(cr.b[:])
	if n == 1 {
		cr.mu.Lock()
		cr.hasByte = true
		cr.mu.Unlock()
		return nil
	}
	if err == nil {
		err = io.ErrNoProgress
	}
	return err
}

func (cr *connReader) startBackgroundRead() {
	cr.mu.Lock()
	if cr.inRead || cr.hasByte {
		cr.mu.Unlock()
		return
	}
	cr.inRead = true
	cr.mu.Unlock()
	// The body has been read, so ReadTimeout is done; left in place it would look like a disconnect.
	_ = cr.c.nc.SetReadDeadline(time.Time{})
	go cr.backgroundRead()
}

func (cr *connReader) backgroundRead() {
	n, err := cr.c.nc.Read(cr.b[:])
	cr.mu.Lock()
	if n == 1 {
		cr.hasByte = true
	}
	aborted := cr.aborted
	cr.inRead = false
	cr.mu.Unlock()
	// The exchange stays valid until abortPendingRead returns, which waits for this goroutine.
	if err != nil && !aborted {
		cr.c.x.Cancel()
	}
	cr.cond.Broadcast()
}

// abortPendingRead stops a background read and waits for it to finish.
func (cr *connReader) abortPendingRead() {
	cr.mu.Lock()
	defer cr.mu.Unlock()
	if !cr.inRead {
		return
	}
	cr.aborted = true
	_ = cr.c.nc.SetReadDeadline(aLongTimeAgo)
	for cr.inRead {
		cr.cond.Wait()
	}
	cr.aborted = false
	_ = cr.c.nc.SetReadDeadline(time.Time{})
}

func getReader(r io.Reader) *bufio.Reader {
	if v := readerPool.Get(); v != nil {
		br := v.(*bufio.Reader)
		br.Reset(r)
		return br
	}
	return bufio.NewReaderSize(r, bufferSize)
}

func putReader(br *bufio.Reader) {
	br.Reset(nil)
	readerPool.Put(br)
}

func getWriter(w io.Writer) *bufio.Writer {
	if v := writerPool.Get(); v != nil {
		bw := v.(*bufio.Writer)
		bw.Reset(w)
		return bw
	}
	return bufio.NewWriterSize(w, bufferSize)
}

func putWriter(bw *bufio.Writer) {
	bw.Reset(nil)
	writerPool.Put(bw)
}

// socketWriter returns what the bufio.Writer writes to: the connection itself, or a stallWriter that bounds each
// write by WriteStallTimeout.
func (c *Conn) socketWriter() io.Writer {
	if c.cfg.WriteStallTimeout <= 0 {
		return c.nc
	}
	if _, ok := c.nc.(io.ReaderFrom); ok {
		return &c.srf
	}
	return &c.sw
}

// stallChunk splits long writes and sendfile calls, so a stall timeout measures a client that stopped reading
// rather than how long a large transfer takes: a client must accept stallChunk bytes per WriteStallTimeout.
const stallChunk = 64 << 10

// stallWriter writes to the connection with a fresh write deadline for each chunk.
type stallWriter struct{ c *Conn }

func (w *stallWriter) Write(p []byte) (int, error) {
	n := 0
	for len(p) > 0 {
		chunk := p[:min(len(p), stallChunk)]
		w.c.armWrite()
		m, err := w.c.nc.Write(chunk)
		n += m
		if err != nil {
			return n, w.c.writeFailed(err)
		}
		p = p[m:]
	}
	return n, nil
}

// stallReaderFrom is a stallWriter that keeps the connection's ReadFrom, so files are still sent with sendfile.
type stallReaderFrom struct{ stallWriter }

func (w *stallReaderFrom) ReadFrom(r io.Reader) (int64, error) {
	rf := w.c.nc.(io.ReaderFrom)
	lr, ok := r.(*io.LimitedReader)
	if !ok {
		w.c.armWrite()
		n, err := rf.ReadFrom(r)
		return n, w.c.writeFailed(err)
	}
	// sendfile looks through one *io.LimitedReader only, so the caller's is shortened per chunk, then restored.
	left := lr.N
	var n int64
	for left > 0 {
		chunk := min(left, stallChunk)
		lr.N = chunk
		w.c.armWrite()
		m, err := rf.ReadFrom(lr)
		n += m
		left -= m
		if err != nil || m < chunk {
			lr.N = left
			return n, w.c.writeFailed(err)
		}
	}
	lr.N = left
	return n, nil
}

func (c *Conn) armWrite() {
	c.writeSet = c.moveDeadline(c.writeSet, c.cfg.WriteStallTimeout, c.writeDeadline, c.nc.SetWriteDeadline)
}

// moveDeadline extends a stall deadline to d from now, capped by the total deadline, and returns it. Moving the
// deadline costs a timer update, so one still at least 7/8 of d away is kept: a stall fails after between 7/8
// of the timeout and all of it.
func (c *Conn) moveDeadline(set time.Time, d time.Duration, total time.Time, apply func(time.Time) error) time.Time {
	now := time.Now()
	if !set.IsZero() && !set.Before(now.Add(d-d/8)) {
		return set
	}
	set = earliest(now.Add(d), total)
	_ = apply(set)
	return set
}

// writeFailed cancels the request when the client stopped accepting data, as when it disconnects.
func (c *Conn) writeFailed(err error) error {
	if errors.Is(err, os.ErrDeadlineExceeded) && c.x != nil {
		c.x.Cancel()
	}
	return err
}

// earliest returns the sooner of two deadlines, where a zero deadline means none.
func earliest(a, b time.Time) time.Time {
	if b.IsZero() || a.Before(b) {
		return a
	}
	return b
}
