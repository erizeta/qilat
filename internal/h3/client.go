package h3

import (
	"bufio"
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/erizeta/qilat/internal/bytesconv"
	"github.com/erizeta/qilat/internal/exchange"
	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/quicvarint"
)

// ClientConfig holds the client settings for HTTP/3 connections, with defaults already resolved.
type ClientConfig struct {
	MaxFieldSectionSize   int
	ResponseHeaderTimeout time.Duration
	IdleTimeout           time.Duration
}

var (
	errMalformedResponse = errors.New("h3: malformed response")
	errRespBodyClosed    = errors.New("h3: response body read after close")
	// ErrConnUnavailable is returned by RoundTrip when the connection no longer takes requests; nothing was sent.
	ErrConnUnavailable = errors.New("h3: connection cannot take more requests")
)

// ClientConn sends requests on one QUIC connection, a stream per request.
type ClientConn struct {
	qc      *quic.Conn
	cfg     *ClientConfig
	onClose func(*ClientConn)
	ctrl    *quic.SendStream

	mu         sync.Mutex
	active     int
	goAway     bool
	goAwayID   quic.StreamID
	closed     bool
	lastActive time.Time
}

// DialFunc opens a connection like net.Dialer.DialContext. HTTP/3 calls it with network "udp".
type DialFunc func(ctx context.Context, network, addr string) (net.Conn, error)

// Dial opens an HTTP/3 connection to addr. tlsConf is cloned and restricted to TLS 1.3 with ALPN "h3".
// A nil dial uses a UDP socket of quic-go's own.
func Dial(ctx context.Context, addr string, tlsConf *tls.Config, cfg *ClientConfig, dial DialFunc,
	onClose func(*ClientConn)) (*ClientConn, error) {
	tc := tlsConf.Clone()
	tc.MinVersion = tls.VersionTLS13
	tc.NextProtos = []string{"h3"}
	qconf := &quic.Config{
		MaxIdleTimeout:        cfg.IdleTimeout,
		MaxIncomingUniStreams: 16, // control and QPACK streams, plus room for unknown types
		MaxIncomingStreams:    -1, // servers do not open request streams; push is not enabled
	}
	var qc *quic.Conn
	var pc net.PacketConn
	var err error
	if dial == nil {
		qc, err = quic.DialAddr(ctx, addr, tc, qconf)
	} else {
		var nc net.Conn
		if nc, err = dial(ctx, "udp", addr); err != nil {
			return nil, err
		}
		pc = connectedPacketConn{nc}
		if qc, err = quic.Dial(ctx, pc, nc.RemoteAddr(), tc, qconf); err != nil {
			_ = pc.Close()
		}
	}
	if err != nil {
		return nil, err
	}
	c := &ClientConn{qc: qc, cfg: cfg, onClose: onClose, lastActive: time.Now()}
	if err := c.openControl(); err != nil {
		_ = qc.CloseWithError(errClosedCriticalStream, "cannot open control stream")
		return nil, err
	}
	go c.acceptUni()
	go func() {
		<-qc.Context().Done()
		c.mu.Lock()
		c.closed = true
		c.mu.Unlock()
		if pc != nil {
			_ = pc.Close() // quic-go leaves a socket it did not create open
		}
		c.onClose(c)
	}()
	return c, nil
}

// connectedPacketConn lets quic-go use a connection from a DialFunc, which is already connected to the
// server. quic-go then uses plain reads and writes instead of batched UDP I/O.
type connectedPacketConn struct{ net.Conn }

func (c connectedPacketConn) ReadFrom(p []byte) (int, net.Addr, error) {
	n, err := c.Read(p)
	return n, c.RemoteAddr(), err
}

func (c connectedPacketConn) WriteTo(p []byte, _ net.Addr) (int, error) { return c.Write(p) }

func (c *ClientConn) openControl() error {
	ctrl, err := c.qc.OpenUniStream()
	if err != nil {
		return err
	}
	c.ctrl = ctrl
	var settings []byte
	settings = quicvarint.Append(settings, settingQPACKMaxTableCapacity)
	settings = quicvarint.Append(settings, 0)
	settings = quicvarint.Append(settings, settingQPACKBlockedStreams)
	settings = quicvarint.Append(settings, 0)
	settings = quicvarint.Append(settings, settingMaxFieldSectionSize)
	settings = quicvarint.Append(settings, uint64(c.cfg.MaxFieldSectionSize)) //nolint:gosec // positive by construction
	b := quicvarint.Append(nil, streamControl)
	b = appendFrameHeader(b, frameSettings, uint64(len(settings)))
	_, err = ctrl.Write(append(b, settings...))
	return err
}

// acceptUni reads the server's control stream for GOAWAY and discards other unidirectional streams.
func (c *ClientConn) acceptUni() {
	ctx := c.qc.Context()
	for {
		rs, err := c.qc.AcceptUniStream(ctx)
		if err != nil {
			return
		}
		go func() {
			br := bufio.NewReader(rs)
			typ, err := quicvarint.Read(br)
			if err != nil {
				return
			}
			if typ != streamControl {
				// QPACK streams carry nothing with a zero-capacity table; others are unknown or push.
				rs.CancelRead(errNoError)
				return
			}
			// The control stream lives as long as the connection, so it only ever ends in an error.
			err = c.readControl(br)
			code := quic.ApplicationErrorCode(errClosedCriticalStream)
			var ce *connError
			if errors.As(err, &ce) {
				code = ce.code
			}
			_ = c.qc.CloseWithError(code, "control stream")
		}()
	}
}

func (c *ClientConn) readControl(br *bufio.Reader) error {
	typ, length, err := readFrameHeader(br)
	if err != nil {
		return err
	}
	if typ != frameSettings {
		return newConnError(errMissingSettings, "first control frame is not SETTINGS")
	}
	if err := skip(br, length); err != nil {
		return err
	}
	for {
		typ, length, err := readFrameHeader(br)
		if err != nil {
			return err
		}
		switch {
		case typ == frameSettings, typ == frameData, typ == frameHeaders, isHTTP2Frame(typ):
			return newConnError(errFrameUnexpected, "unexpected frame on control stream")
		case typ == frameGoAway:
			if length > 8 {
				return newConnError(errFrame, "oversized GOAWAY")
			}
			id, err := quicvarint.Read(br)
			if err != nil {
				return err
			}
			c.mu.Lock()
			c.goAway, c.goAwayID = true, quic.StreamID(id) //nolint:gosec // a varint fits in a stream ID
			c.mu.Unlock()
			continue
		}
		if err := skip(br, length); err != nil {
			return err
		}
	}
}

// CanTakeRequest reports whether the connection accepts new requests.
func (c *ClientConn) CanTakeRequest() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return !c.closed && !c.goAway
}

// IdleFor returns how long the connection has had no requests, or zero while it has some.
func (c *ClientConn) IdleFor(now time.Time) time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.active > 0 {
		return 0
	}
	return now.Sub(c.lastActive)
}

// Close closes the connection.
func (c *ClientConn) Close() error { return c.qc.CloseWithError(errNoError, "") }

// CloseIfIdle closes the connection if no request is active, and reports whether it did.
func (c *ClientConn) CloseIfIdle() bool {
	c.mu.Lock()
	idle := c.active == 0
	c.mu.Unlock()
	if idle {
		_ = c.Close()
	}
	return idle
}

// RoundTrip sends req on a new stream and reads the response header into resp.
func (c *ClientConn) RoundTrip(ctx context.Context, req *exchange.ClientRequest, resp exchange.ClientResponse) error {
	c.mu.Lock()
	if c.closed || c.goAway {
		c.mu.Unlock()
		return ErrConnUnavailable
	}
	c.active++
	c.mu.Unlock()
	str, err := c.qc.OpenStreamSync(ctx)
	if err != nil {
		c.streamDone()
		if ctx.Err() != nil {
			return context.Cause(ctx)
		}
		return ErrConnUnavailable
	}
	s := &clientStream{c: c, str: str, resp: resp, head: req.Method == "HEAD", ctx: ctx}
	s.br = bufio.NewReader(str)
	if ctx.Done() != nil {
		s.stop = context.AfterFunc(ctx, func() {
			str.CancelRead(errRequestCancelled)
			str.CancelWrite(errRequestCancelled)
		})
	}
	if err := s.writeHeaders(req); err != nil {
		return s.fail(err)
	}
	if req.Body != nil {
		// The body is sent while the response is read, so a server that answers as it reads cannot stall
		// on a full receive window.
		s.writing = make(chan struct{})
		go s.writeBody(req.Body)
	}
	if err := s.readHeader(); err != nil {
		return s.fail(err)
	}
	if s.bodyless() {
		resp.SetBody(nil, 0)
		s.finish()
		return nil
	}
	resp.SetBody(s, s.contentLen)
	return nil
}

func (c *ClientConn) streamDone() {
	c.mu.Lock()
	c.active--
	if c.active == 0 {
		c.lastActive = time.Now()
	}
	c.mu.Unlock()
}

// clientStream is one request and its response.
type clientStream struct {
	c          *ClientConn
	str        *quic.Stream
	br         *bufio.Reader
	resp       exchange.ClientResponse
	ctx        context.Context
	stop       func() bool
	head       bool
	status     int
	contentLen int64
	remaining  uint64 // bytes left in the current DATA frame
	eof        bool
	err        error
	done       bool
	dec        decoder
	buf        []byte
	writing    chan struct{} // closed when the body writer goroutine returns; nil without a body
}

func (s *clientStream) writeHeaders(req *exchange.ClientRequest) error {
	sec := appendSectionPrefix(nil)
	sec = appendField(sec, nameMethod, bytesconv.S2B(req.Method))
	sec = appendField(sec, nameScheme, bytesconv.S2B(req.Scheme))
	sec = appendField(sec, nameAuthority, req.Authority)
	sec = appendField(sec, namePath, req.Target)
	for i := range req.Header.Len() {
		name, value := req.Header.At(i)
		switch string(name) {
		case "host", "connection", "keep-alive", "proxy-connection", "transfer-encoding", "upgrade",
			"content-length", "te":
			continue // connection-specific fields are not allowed in HTTP/3 (RFC 9114 section 4.2)
		}
		sec = appendField(sec, name, value)
	}
	if req.Body != nil && req.ContentLength >= 0 {
		var n [20]byte
		sec = appendField(sec, nameContentLength, strconv.AppendInt(n[:0], req.ContentLength, 10))
	}
	b := appendFrameHeader(make([]byte, 0, len(sec)+16), frameHeaders, uint64(len(sec)))
	b = append(b, sec...)
	if _, err := s.str.Write(b); err != nil {
		return err
	}
	if req.Body == nil {
		return s.str.Close()
	}
	return nil
}

// writeBody sends body as DATA frames and ends the request stream. It runs in its own goroutine.
func (s *clientStream) writeBody(body io.Reader) {
	defer close(s.writing)
	buf := make([]byte, 16<<10)
	var hdr [16]byte
	for {
		n, err := body.Read(buf)
		if n > 0 {
			if _, werr := s.str.Write(appendFrameHeader(hdr[:0], frameData, uint64(n))); werr != nil {
				return
			}
			if _, werr := s.str.Write(buf[:n]); werr != nil {
				return
			}
		}
		if errors.Is(err, io.EOF) {
			_ = s.str.Close()
			return
		}
		if err != nil {
			s.str.CancelWrite(errRequestCancelled)
			return
		}
	}
}

var (
	nameMethod        = []byte(":method")
	nameScheme        = []byte(":scheme")
	nameAuthority     = []byte(":authority")
	namePath          = []byte(":path")
	nameContentLength = []byte("content-length")
)

// readHeader reads frames until the final response header, skipping informational responses.
func (s *clientStream) readHeader() error {
	if d := s.c.cfg.ResponseHeaderTimeout; d > 0 {
		_ = s.str.SetReadDeadline(time.Now().Add(d))
		defer func() { _ = s.str.SetReadDeadline(time.Time{}) }()
	}
	for {
		typ, length, err := readFrameHeader(s.br)
		if err != nil {
			return err
		}
		switch {
		case typ == frameHeaders:
			if length > uint64(s.c.cfg.MaxFieldSectionSize) { //nolint:gosec // positive by construction
				return errMalformedResponse
			}
			p, err := s.readPayload(length)
			if err != nil {
				return err
			}
			sink := headerSink{s: s}
			if err := s.dec.decode(p, &sink); err != nil {
				return err
			}
			if sink.err != nil {
				return sink.err
			}
			if s.status >= 200 {
				s.resp.SetStatus(s.status, 3, 0)
				return nil
			}
		case typ == frameData, isHTTP2Frame(typ), typ == frameSettings, typ == frameGoAway, typ == framePushPromise:
			return errMalformedResponse
		default:
			if err := skip(s.br, length); err != nil {
				return err
			}
		}
	}
}

func (s *clientStream) readPayload(n uint64) ([]byte, error) {
	if uint64(cap(s.buf)) < n {
		s.buf = make([]byte, n)
	}
	p := s.buf[:n]
	if _, err := io.ReadFull(s.br, p); err != nil {
		return nil, err
	}
	return p, nil
}

// headerSink receives a decoded response header or trailer section.
type headerSink struct {
	s       *clientStream
	trailer bool
	n       int
	err     error
}

func (h *headerSink) Field(name, value []byte) {
	if h.err != nil {
		return
	}
	s := h.s
	first := h.n == 0
	h.n++
	if h.trailer {
		if len(name) > 0 && name[0] == ':' {
			h.err = errMalformedResponse
			return
		}
		h.err = s.resp.AddTrailer(name, value)
		return
	}
	if first {
		if string(name) != ":status" || len(value) != 3 {
			h.err = errMalformedResponse
			return
		}
		st, err := strconv.Atoi(bytesconv.B2S(value))
		if err != nil || st < 100 {
			h.err = errMalformedResponse
			return
		}
		s.status = st
		s.contentLen = -1
		return
	}
	if len(name) == 0 || name[0] == ':' {
		h.err = errMalformedResponse
		return
	}
	if s.status < 200 {
		return
	}
	if string(name) == "content-length" {
		n, ok := bytesconv.ParseUint(value)
		if !ok {
			h.err = errMalformedResponse
			return
		}
		s.contentLen = n
	}
	h.err = s.resp.AddHeader(name, value)
}

func (s *clientStream) bodyless() bool {
	return s.head || s.status == 204 || s.status == 304
}

// Read reads the response body from DATA frames, collecting trailers at the end.
func (s *clientStream) Read(p []byte) (int, error) {
	if s.err != nil {
		return 0, s.err
	}
	if s.eof {
		return 0, io.EOF
	}
	for s.remaining == 0 {
		typ, length, err := readFrameHeader(s.br)
		if errors.Is(err, io.EOF) {
			return 0, s.end(io.EOF)
		}
		if err != nil {
			return 0, s.end(err)
		}
		switch {
		case typ == frameData:
			s.remaining = length
		case typ == frameHeaders:
			payload, err := s.readPayload(length)
			if err != nil {
				return 0, s.end(err)
			}
			sink := headerSink{s: s, trailer: true}
			if err := s.dec.decode(payload, &sink); err != nil || sink.err != nil {
				return 0, s.end(errMalformedResponse)
			}
			// Nothing may follow the trailers (RFC 9114 section 4.1).
			if _, err := s.br.Peek(1); !errors.Is(err, io.EOF) {
				return 0, s.end(errMalformedResponse)
			}
			return 0, s.end(io.EOF)
		case isHTTP2Frame(typ), typ == frameSettings, typ == frameGoAway, typ == framePushPromise:
			return 0, s.end(errMalformedResponse)
		default:
			if err := skip(s.br, length); err != nil {
				return 0, s.end(err)
			}
		}
	}
	n, err := s.br.Read(p[:min(uint64(len(p)), s.remaining)])
	s.remaining -= uint64(n) //nolint:gosec // n is never negative
	if errors.Is(err, io.EOF) {
		if s.remaining > 0 {
			return n, s.end(io.ErrUnexpectedEOF)
		}
		err = nil // the end of the stream is seen on the next Read
	}
	if err != nil {
		return n, s.end(err)
	}
	return n, nil
}

// end finishes the body with err, which is io.EOF on success.
func (s *clientStream) end(err error) error {
	if errors.Is(err, io.EOF) {
		s.eof = true
	} else {
		if s.ctx.Err() != nil {
			err = context.Cause(s.ctx)
		}
		s.err = err
		s.str.CancelRead(errRequestCancelled)
	}
	s.finish()
	return err
}

// Close abandons the rest of the body.
func (s *clientStream) Close() error {
	if !s.eof && s.err == nil {
		s.err = errRespBodyClosed
		s.str.CancelRead(errRequestCancelled)
		s.str.CancelWrite(errRequestCancelled)
	}
	s.finish()
	return nil
}

// fail ends a request that produced no response.
func (s *clientStream) fail(err error) error {
	s.str.CancelRead(errRequestCancelled)
	s.str.CancelWrite(errRequestCancelled)
	var se *quic.StreamError
	switch {
	case s.ctx.Err() != nil:
		err = context.Cause(s.ctx)
	case errors.As(err, &se) && se.Remote && se.ErrorCode == errRequestRejected:
		// The server refused the request before acting on it (RFC 9114 section 4.1.1).
		err = exchange.NotProcessed(err)
	case s.aboveGoAway():
		err = exchange.NotProcessed(err)
	}
	s.finish()
	return err
}

func (s *clientStream) aboveGoAway() bool {
	c := s.c
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.goAway && s.str.StreamID() >= c.goAwayID
}

func (s *clientStream) finish() {
	if s.done {
		return
	}
	s.done = true
	if s.writing != nil {
		select {
		case <-s.writing:
		default:
			// The response is over, so the rest of the upload is not wanted. Waiting means the caller's
			// body is never read after the response is released.
			s.str.CancelWrite(errRequestCancelled)
			<-s.writing
		}
	}
	if s.stop != nil {
		s.stop()
	}
	s.c.streamDone()
}
