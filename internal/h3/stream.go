package h3

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/erizeta/qilat/internal/exchange"
	"github.com/erizeta/qilat/internal/httpfields"
	"github.com/erizeta/qilat/internal/stall"
	"github.com/quic-go/quic-go"
)

const readerSize = 4 << 10

// stream is one request/response on a bidirectional QUIC stream. It is pooled with its buffers.
type stream struct {
	c   *Conn
	str *quic.Stream
	br  *bufio.Reader // reads through r
	r   streamReader
	x   exchange.Exchange
	req httpfields.Request
	dec decoder
	in  []byte // HEADERS payloads read
	sec []byte // field section being encoded
	out []byte // frames being written

	readDeadline  time.Time // from ReadTimeout; zero when unset
	writeDeadline time.Time // from WriteTimeout; zero when unset
	inBody        bool      // the header has been read, so reads are watched by readStall

	// readStall and writeStall reset the stream when a read or write waits a whole stall timeout. They are
	// pooled with the stream, so watching does not allocate; stalled* tell the waiting call why it failed.
	readStall, writeStall     stall.Watchdog
	readStalled, writeStalled atomic.Bool

	// cancelMu orders a stream reset that cancels the exchange against its release to the pool. gen tells a
	// late reset callback from an earlier request apart, since streams and exchanges are both reused.
	cancelMu sync.Mutex
	live     exchange.Exchange
	gen      uint64

	body body
	sink sink
}

var streamPool = sync.Pool{New: func() any {
	s := &stream{br: bufio.NewReaderSize(nil, readerSize)}
	s.r.s = s
	s.body.s = s
	s.sink.s = s
	s.readStall.Init(func() { s.stall(&s.readStalled, (*quic.Stream).CancelRead) })
	s.writeStall.Init(func() { s.stall(&s.writeStalled, (*quic.Stream).CancelWrite) })
	return s
}}

func (c *Conn) serveStream(str *quic.Stream) {
	defer c.wg.Done()
	defer c.streamDone()
	s := streamPool.Get().(*stream)
	s.c, s.str = c, str
	s.br.Reset(&s.r)
	s.body = body{s: s, declared: -1, limit: c.cfg.MaxRequestBodySize}
	s.sink = sink{s: s}
	defer s.release()

	now := time.Now()
	s.readDeadline, s.writeDeadline, s.inBody = time.Time{}, time.Time{}, false
	s.readStalled.Store(false)
	s.writeStalled.Store(false)
	s.readStall.SetTimeout(c.cfg.ReadStallTimeout)
	s.writeStall.SetTimeout(c.cfg.WriteStallTimeout)
	if d := c.cfg.ReadTimeout; d > 0 {
		s.readDeadline = now.Add(d)
	}
	if d := c.cfg.WriteTimeout; d > 0 {
		s.writeDeadline = now.Add(d)
		_ = str.SetWriteDeadline(s.writeDeadline)
	}
	// A client may open a stream and send nothing; the header is bounded as on the other protocols.
	if d := c.cfg.ReadHeaderTimeout; d > 0 {
		_ = str.SetReadDeadline(earliest(now.Add(d), s.readDeadline))
	} else {
		_ = str.SetReadDeadline(s.readDeadline)
	}
	if err := s.serve(); err != nil {
		var ce *connError
		if errors.As(err, &ce) {
			c.closeOnError(err)
			return
		}
		str.CancelRead(errRequestIncomplete)
		str.CancelWrite(errRequestIncomplete)
	}
}

func (s *stream) release() {
	s.c, s.str, s.x = nil, nil, nil
	s.br.Reset(nil)
	streamPool.Put(s)
}

// serve reads the request header, runs the handler, and finishes the stream.
func (s *stream) serve() error {
	c := s.c
	length, err := s.firstHeaders()
	if err != nil {
		return err
	}
	// The encoded section can be smaller than the decoded one, so allow some slack before reading it.
	if length > uint64(c.cfg.MaxFieldSectionSize)*2 {
		return s.reject(431)
	}
	x := c.cfg.Acquire()
	s.x = x
	defer x.Release()
	s.req.Reset(x, false, c.cfg.MaxFieldSectionSize)
	if err := s.readSection(length); err != nil {
		return err
	}
	s.inBody = true
	_ = s.str.SetReadDeadline(s.readDeadline)
	switch {
	case s.req.TooLarge:
		return s.reject(431)
	case s.req.Finish(3) != "":
		s.str.CancelRead(errMessage)
		s.str.CancelWrite(errMessage)
		return nil
	case c.cfg.MaxRequestBodySize >= 0 && s.req.ContentLength > c.cfg.MaxRequestBodySize:
		return s.reject(413)
	}
	s.str.SetPriority(int8(s.req.Urgency), s.req.Incremental) //nolint:gosec // urgency is 0 to 7
	x.SetConn(c.qc.RemoteAddr(), c.tlsState())
	s.body.declared = s.req.ContentLength
	s.body.expectContinue = s.req.ExpectContinue
	if s.req.ContentLength == 0 {
		x.SetBody(nil, 0)
	} else {
		x.SetBody(&s.body, s.req.ContentLength)
	}
	// A request in 0-RTT data can be replayed by an attacker, so only read-only ones are served early.
	if c.cfg.Allow0RTT && !c.handshakeDone() && !allowedEarly(s.req.Method()) {
		return s.reject(425)
	}

	// The stream's context ends when the client stops reading the response (STOP_SENDING) or the stream is reset.
	s.cancelMu.Lock()
	s.live = x
	s.gen++
	gen := s.gen
	s.cancelMu.Unlock()
	stop := context.AfterFunc(s.str.Context(), func() { s.cancel(gen) })
	serveErr := x.Serve(&s.sink)
	stop()
	s.cancelMu.Lock()
	s.live = nil
	s.cancelMu.Unlock()
	if serveErr != nil && !s.sink.ended {
		s.str.CancelWrite(errInternal)
	}
	// A protocol violation found while the handler read the body ends the whole connection.
	if ce := (*connError)(nil); errors.As(s.body.err, &ce) {
		return ce
	}
	if !s.body.eof {
		// The response is complete; the rest of the request body is not needed (RFC 9114 section 4.1).
		s.str.CancelRead(errNoError)
	}
	return nil
}

func (s *stream) cancel(gen uint64) {
	s.cancelMu.Lock()
	if s.live != nil && s.gen == gen {
		s.live.Cancel()
	}
	s.cancelMu.Unlock()
}

// allowedEarly reports whether a request with this method may be served from 0-RTT data (RFC 8470 section 4).
// PUT and DELETE are idempotent, but a replay delivered later could undo a change made since.
func allowedEarly(method []byte) bool {
	switch string(method) {
	case "GET", "HEAD", "OPTIONS":
		return true
	}
	return false
}

// firstHeaders skips unknown frames until the request's HEADERS frame and returns its length.
func (s *stream) firstHeaders() (uint64, error) {
	skipped := uint64(0)
	for {
		typ, length, err := readFrameHeader(s.br)
		if err != nil {
			return 0, err
		}
		switch {
		case typ == frameHeaders:
			return length, nil
		case typ == frameData, typ == frameSettings, typ == frameGoAway, typ == frameMaxPushID,
			typ == frameCancelPush, typ == framePushPromise, isHTTP2Frame(typ):
			return 0, newConnError(errFrameUnexpected, "unexpected frame before HEADERS")
		}
		if skipped += length; skipped > 1<<16 {
			return 0, newConnError(errExcessiveLoad, "too many unknown frames")
		}
		if err := skip(s.br, length); err != nil {
			return 0, err
		}
	}
}

// readSection reads a HEADERS payload and decodes it into s.req.
func (s *stream) readSection(length uint64) error {
	if uint64(cap(s.in)) < length {
		s.in = make([]byte, length)
	}
	p := s.in[:length]
	if _, err := io.ReadFull(s.br, p); err != nil {
		return err
	}
	if err := s.dec.decode(p, &s.req); err != nil {
		return newConnError(errQPACKDecompression, "invalid field section")
	}
	return nil
}

// reject answers with an error status without running a handler.
func (s *stream) reject(status int) error {
	text := statusText(status)
	sec := appendSectionPrefix(s.sec[:0])
	sec = appendStatus(sec, status)
	sec = appendField(sec, contentType, textPlain)
	var n [8]byte
	sec = appendField(sec, contentLength, strconv.AppendInt(n[:0], int64(len(text)), 10))
	s.sec = sec
	b := appendFrameHeader(s.out[:0], frameHeaders, uint64(len(sec)))
	b = append(b, sec...)
	b = appendFrameHeader(b, frameData, uint64(len(text)))
	b = append(b, text...)
	s.out = b
	s.str.CancelRead(errNoError)
	if _, err := s.str.Write(b); err != nil {
		return nil //nolint:nilerr // the client is gone; nothing more to do
	}
	_ = s.str.Close()
	return nil
}

func statusText(status int) string {
	switch status {
	case 413:
		return "413 Request Entity Too Large"
	case 425:
		return "425 Too Early"
	}
	return "431 Request Header Fields Too Large"
}

var (
	contentType   = []byte("content-type")
	contentLength = []byte("content-length")
	textPlain     = []byte("text/plain; charset=utf-8")
)

// body reads DATA frames of a request. Trailers are validated and discarded.
type body struct {
	s              *stream
	remaining      uint64 // left in the current DATA frame
	eof            bool
	err            error
	total          int64
	declared       int64
	limit          int64
	expectContinue bool
}

func (b *body) Read(p []byte) (int, error) {
	if b.err != nil {
		return 0, b.err
	}
	if b.expectContinue {
		b.expectContinue = false
		if !b.s.sink.headerSent {
			_ = b.s.sink.WriteInformational(100, nil)
		}
	}
	for b.remaining == 0 {
		if b.eof {
			return 0, io.EOF
		}
		if err := b.nextFrame(); err != nil {
			return 0, b.fail(err)
		}
	}
	n, err := b.s.br.Read(p[:min(uint64(len(p)), b.remaining)])
	b.remaining -= uint64(n)
	b.total += int64(n)
	if errors.Is(err, io.EOF) && b.remaining == 0 {
		// QUIC can deliver the last bytes and the end of the stream together; the frame is complete.
		err = nil
		b.eof = true
		if b.declared >= 0 && b.total != b.declared {
			return n, b.fail(errBodyLength)
		}
	}
	switch {
	case b.limit >= 0 && b.total > b.limit:
		if b.s.str != nil {
			b.s.str.CancelRead(errExcessiveLoad)
		}
		return n, b.fail(exchange.ErrBodyTooLarge)
	case b.declared >= 0 && b.total > b.declared:
		return n, b.fail(errBodyLength)
	case err != nil:
		if errors.Is(err, io.EOF) {
			err = io.ErrUnexpectedEOF
		}
		return n, b.fail(err)
	}
	return n, nil
}

var errBodyLength = errors.New("qilat: HTTP/3 request body does not match content-length")

func (b *body) fail(err error) error {
	b.err = err
	if errors.Is(err, errBodyLength) || errors.Is(err, errMalformedTrailers) {
		// A malformed request is a stream error even if the handler ignores it (RFC 9114 section 4.1.2).
		if b.s.str != nil {
			b.s.str.CancelRead(errMessage)
			b.s.str.CancelWrite(errMessage)
		}
	}
	var se *quic.StreamError
	if errors.As(err, &se) && se.Remote {
		b.s.x.Cancel() // the client reset the request
	}
	return err
}

// nextFrame reads the next frame header, consuming trailers and ignoring unknown frames.
func (b *body) nextFrame() error {
	s := b.s
	typ, length, err := readFrameHeader(s.br)
	if errors.Is(err, io.EOF) {
		if b.declared >= 0 && b.total != b.declared {
			return errBodyLength
		}
		b.eof = true
		return nil
	}
	if err != nil {
		return err
	}
	switch {
	case typ == frameData:
		b.remaining = length
		return nil
	case typ == frameHeaders:
		if length > uint64(s.c.cfg.MaxFieldSectionSize)*2 {
			return newConnError(errExcessiveLoad, "trailers too large")
		}
		s.req.Reset(nil, true, s.c.cfg.MaxFieldSectionSize)
		if err := s.readSection(length); err != nil {
			return err
		}
		if s.req.Malformed != "" || s.req.TooLarge {
			return errMalformedTrailers
		}
		// Trailers end the request: anything but the end of the stream is an error.
		if _, _, err := readFrameHeader(s.br); !errors.Is(err, io.EOF) {
			return newConnError(errFrameUnexpected, "frame after trailers")
		}
		return b.nextFrame()
	case typ == frameSettings, typ == frameGoAway, typ == frameMaxPushID, typ == frameCancelPush,
		typ == framePushPromise, isHTTP2Frame(typ):
		return newConnError(errFrameUnexpected, "unexpected frame on request stream")
	}
	return skip(s.br, length)
}

var (
	errMalformedTrailers = errors.New("qilat: malformed HTTP/3 request trailers")
	// errStreamStalled is returned to a handler whose client stopped sending the body or accepting the response.
	errStreamStalled = fmt.Errorf("qilat: HTTP/3 stream stalled: %w", os.ErrDeadlineExceeded)
)

// sink writes the response of one stream.
type sink struct {
	s          *stream
	headerSent bool
	ended      bool
}

var (
	_ exchange.ResponseSink      = (*sink)(nil)
	_ exchange.InformationalSink = (*sink)(nil)
	_ exchange.TrailerSink       = (*sink)(nil)
	_ exchange.CompleteSink      = (*sink)(nil)
)

// appendHeaders appends a HEADERS frame; status 0 encodes trailers.
func (w *sink) appendHeaders(b []byte, status int, h exchange.HeaderList) []byte {
	sec := appendSectionPrefix(w.s.sec[:0])
	if status != 0 {
		sec = appendStatus(sec, status)
	}
	hasDate := false
	if h != nil {
		for i := range h.Len() {
			name, value := h.At(i)
			if httpfields.SkipResponse(name, status == 0) {
				continue
			}
			if string(name) == "date" {
				hasDate = true
			}
			sec = appendField(sec, name, value)
		}
	}
	if status >= 200 && !hasDate {
		sec = appendField(sec, httpfields.DateName, httpfields.Date())
	}
	w.s.sec = sec
	b = appendFrameHeader(b, frameHeaders, uint64(len(sec)))
	return append(b, sec...)
}

func (w *sink) write(b []byte) error {
	w.s.out = b
	_, err := w.s.writeStream(b)
	return err
}

func (w *sink) WriteHeader(status int, h exchange.HeaderList, _ int64) error {
	w.headerSent = true
	return w.write(w.appendHeaders(w.s.out[:0], status, h))
}

func (w *sink) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	var hdr [16]byte
	if _, err := w.s.writeStream(appendFrameHeader(hdr[:0], frameData, uint64(len(p)))); err != nil {
		return 0, err
	}
	return w.s.writeStream(p)
}

// Flush is a no-op: QUIC streams send buffered data as soon as flow control allows.
func (w *sink) Flush() error { return nil }

func (w *sink) Finish() error {
	w.ended = true
	return w.s.str.Close()
}

func (w *sink) FinishTrailer(trailer exchange.HeaderList) error {
	w.ended = true
	if err := w.write(w.appendHeaders(w.s.out[:0], 0, trailer)); err != nil {
		return err
	}
	return w.s.str.Close()
}

func (w *sink) WriteComplete(status int, h exchange.HeaderList, _ int64, p []byte, trailer exchange.HeaderList) error {
	w.headerSent, w.ended = true, true
	b := w.appendHeaders(w.s.out[:0], status, h)
	if len(p) > 0 {
		b = appendFrameHeader(b, frameData, uint64(len(p)))
		b = append(b, p...)
	}
	if trailer != nil && trailer.Len() > 0 {
		b = w.appendHeaders(b, 0, trailer)
	}
	if err := w.write(b); err != nil {
		return err
	}
	return w.s.str.Close()
}

func (w *sink) WriteInformational(status int, h exchange.HeaderList) error {
	if w.headerSent {
		return nil
	}
	if status == 100 {
		w.s.body.expectContinue = false
	}
	return w.write(w.appendHeaders(w.s.out[:0], status, h))
}

// streamReader is what the stream's bufio.Reader reads from. Once the header is read, each read is watched by
// readStall, so a body the client stops sending fails without limiting a slow upload that keeps going.
type streamReader struct{ s *stream }

func (r *streamReader) Read(p []byte) (int, error) {
	s := r.s
	if !s.inBody {
		return s.str.Read(p)
	}
	s.readStall.Begin()
	n, err := s.str.Read(p)
	s.readStall.End()
	if err != nil && s.readStalled.Load() {
		err = errStreamStalled
	}
	return n, err
}

// stallChunk splits long writes, so a stall timeout measures a client that stopped reading rather than how long
// a large response takes: a client must accept stallChunk bytes per WriteStallTimeout.
const stallChunk = 64 << 10

// writeStream writes p to the stream, watching each chunk with writeStall.
func (s *stream) writeStream(p []byte) (int, error) {
	if s.c.cfg.WriteStallTimeout <= 0 {
		return s.str.Write(p)
	}
	n := 0
	for len(p) > 0 {
		chunk := p[:min(len(p), stallChunk)]
		s.writeStall.Begin()
		m, err := s.str.Write(chunk)
		s.writeStall.End()
		n += m
		if err != nil {
			if s.writeStalled.Load() {
				err = errStreamStalled
			}
			return n, err
		}
		p = p[m:]
	}
	return n, nil
}

// stall resets one direction of the stream after its client stopped sending or accepting data, and cancels
// the request as if the client had reset it. A stream back in the pool, or serving a later request, is safe
// from a late call: only the request still being served is canceled.
func (s *stream) stall(flag *atomic.Bool, cancel func(*quic.Stream, quic.StreamErrorCode)) {
	s.cancelMu.Lock()
	defer s.cancelMu.Unlock()
	if s.live == nil {
		return
	}
	// The context is canceled before the waiting call wakes, so the handler sees both together.
	flag.Store(true)
	s.live.Cancel()
	cancel(s.str, errRequestCancelled)
}

// earliest returns the sooner of two deadlines, where a zero deadline means none.
func earliest(a, b time.Time) time.Time {
	if b.IsZero() || a.Before(b) {
		return a
	}
	return b
}
