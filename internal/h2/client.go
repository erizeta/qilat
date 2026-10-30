package h2

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/erizeta/qilat/internal/bytesconv"
	"github.com/erizeta/qilat/internal/exchange"
	"golang.org/x/net/http2/hpack"
)

// ClientConfig holds the client settings for HTTP/2 connections, with defaults already resolved.
type ClientConfig struct {
	MaxHeaderListSize uint32
	// StreamWindow is the receive window per stream, which bounds how much of an unread body is buffered.
	StreamWindow          uint32
	ResponseHeaderTimeout time.Duration
}

const (
	clientConnWindow = 1 << 30 // the connection window is replenished as data arrives; streams do the limiting
	clientMaxFrame   = 1 << 14
	// peerStreamsBeforeSettings is how many streams are opened before the server's SETTINGS say how many it allows.
	peerStreamsBeforeSettings = 100
)

var (
	errClientConnClosed = errors.New("h2: client connection closed")
	errStreamRefused    = errors.New("h2: stream refused by server")
	// ErrConnUnavailable is returned by RoundTrip when the connection cannot open another stream; nothing was sent.
	ErrConnUnavailable = errors.New("h2: connection cannot take more streams")
	errMalformedResp   = errors.New("h2: malformed response header")
	errBodyAfterClose  = errors.New("h2: response body read after close")
)

// ClientConn sends requests as streams multiplexed on one HTTP/2 connection. Its reader goroutine runs until
// the connection closes; onClose is then called once.
type ClientConn struct {
	nc      net.Conn
	cfg     *ClientConfig
	onClose func(*ClientConn)

	wmu  sync.Mutex // serializes frames; stream IDs are allocated under it so they go out in order
	bw   *bufio.Writer
	enc  *encoder
	wbuf []byte
	fhdr [frameHeaderLen]byte

	// Reader goroutine only.
	fr      framer
	dec     *hpack.Decoder
	fields  []hpack.HeaderField
	blockID uint32
	blockN  int
	blockES bool

	mu          sync.Mutex
	flow        sync.Cond // send windows grew, or a stream or the connection ended
	streams     map[uint32]*clientStream
	nextID      uint32
	maxStreams  uint32
	active      int
	sendWindow  int64
	peerWindow  int64
	peerFrame   uint32
	goAway      bool
	closed      bool
	err         error
	lastActive  time.Time
	connUnacked int64 // DATA received and not yet returned to the server's connection window
}

// NewClientConn starts HTTP/2 on nc, whose TLS handshake negotiated "h2".
func NewClientConn(nc net.Conn, cfg *ClientConfig, onClose func(*ClientConn)) (*ClientConn, error) {
	c := &ClientConn{
		nc: nc, cfg: cfg, onClose: onClose,
		bw:         bufio.NewWriterSize(nc, writeBufferSize),
		enc:        newEncoder(defaultHeaderTable),
		streams:    make(map[uint32]*clientStream),
		nextID:     1,
		maxStreams: peerStreamsBeforeSettings,
		sendWindow: defaultWindow,
		peerWindow: defaultWindow,
		peerFrame:  minMaxFrameSize,
		lastActive: time.Now(),
	}
	c.flow.L = &c.mu
	c.fr = framer{r: bufio.NewReaderSize(nc, writeBufferSize), maxRead: clientMaxFrame}
	c.dec = newDecoder(defaultHeaderTable, int(cfg.MaxHeaderListSize), func(f hpack.HeaderField) {
		c.fields = append(c.fields, f)
	})

	b := append([]byte(nil), ClientPreface...)
	settings := []byte{}
	settings = appendSetting(settings, settingEnablePush, 0)
	settings = appendSetting(settings, settingInitialWindowSize, cfg.StreamWindow)
	settings = appendSetting(settings, settingMaxFrameSize, clientMaxFrame)
	settings = appendSetting(settings, settingMaxHeaderListSize, cfg.MaxHeaderListSize)
	b = appendFrameHeader(b, len(settings), frameSettings, 0, 0)
	b = append(b, settings...)
	b = appendFrameHeader(b, 4, frameWindowUpdate, 0, 0)
	b = appendBE32(b, clientConnWindow-defaultWindow)
	if _, err := nc.Write(b); err != nil {
		return nil, err
	}
	go c.readLoop()
	return c, nil
}

// CanTakeRequest reports whether a new stream can be opened now.
func (c *ClientConn) CanTakeRequest() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return !c.closed && !c.goAway && c.active < int(c.maxStreams) && c.nextID < maxStreamID
}

// Usable reports whether the connection can still open streams once some finish.
func (c *ClientConn) Usable() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return !c.closed && !c.goAway && c.nextID < maxStreamID
}

// IdleFor returns how long the connection has had no streams, or zero while it has some.
func (c *ClientConn) IdleFor(now time.Time) time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.active > 0 {
		return 0
	}
	return now.Sub(c.lastActive)
}

// Close closes the connection, failing its streams.
func (c *ClientConn) Close() error {
	c.shutdown(errClientConnClosed)
	return nil
}

// CloseIfIdle closes the connection if it has no streams, and reports whether it did.
func (c *ClientConn) CloseIfIdle() bool {
	c.mu.Lock()
	idle := c.active == 0
	c.mu.Unlock()
	if idle {
		c.shutdown(errClientConnClosed)
	}
	return idle
}

// shutdown closes the connection once; the reader goroutine then fails every stream.
func (c *ClientConn) shutdown(err error) {
	c.mu.Lock()
	if c.err == nil {
		c.err = err
	}
	c.mu.Unlock()
	_ = c.nc.Close()
}

// clientStream is one request and its response.
type clientStream struct {
	c    *ClientConn
	id   uint32
	resp exchange.ClientResponse
	cond sync.Cond // on c.mu: headers or body data arrived, or the stream ended

	sendWindow int64
	head       bool
	gotHeaders bool
	endStream  bool // the server finished sending
	sentEnd    bool // we finished sending
	err        error
	done       bool // removed from the connection
	buf        []byte
	off        int
	unacked    uint32 // body bytes read by the caller and not yet returned to the server's window
	contentLen int64
	status     int
	stopCancel func() bool
	writing    chan struct{} // closed when the body writer goroutine returns; nil without a body
}

// RoundTrip sends req on a new stream and waits for the response header.
func (c *ClientConn) RoundTrip(ctx context.Context, req *exchange.ClientRequest, resp exchange.ClientResponse) error {
	s := &clientStream{c: c, resp: resp, head: req.Method == "HEAD", contentLen: -1}
	s.cond.L = &c.mu

	c.wmu.Lock()
	c.mu.Lock()
	if c.closed || c.goAway || c.active >= int(c.maxStreams) || c.nextID >= maxStreamID {
		c.mu.Unlock()
		c.wmu.Unlock()
		return ErrConnUnavailable
	}
	s.id = c.nextID
	c.nextID += 2
	s.sendWindow = c.peerWindow
	c.streams[s.id] = s
	c.active++
	c.mu.Unlock()
	endStream := req.Body == nil
	err := c.writeRequestHeaders(s.id, req, endStream)
	c.wmu.Unlock()
	if err != nil {
		c.shutdown(err)
		return exchange.NotProcessed(err)
	}
	if endStream {
		s.sentEnd = true
	}
	if ctx.Done() != nil {
		s.stopCancel = context.AfterFunc(ctx, func() { s.abort(context.Cause(ctx)) })
	}
	if req.Body != nil {
		// The body is sent while the response is read, so a server that answers as it reads cannot stall
		// on a full receive window.
		body := req.Body
		s.writing = make(chan struct{})
		go func() {
			defer close(s.writing)
			if err := s.writeBody(body); err != nil {
				s.abort(err)
			}
		}()
	}

	var timer *time.Timer
	if d := c.cfg.ResponseHeaderTimeout; d > 0 {
		timer = time.AfterFunc(d, func() { s.abort(errors.New("h2: timeout awaiting response headers")) })
	}
	c.mu.Lock()
	for !s.gotHeaders && s.err == nil {
		s.cond.Wait()
	}
	err = s.err
	gotHeaders := s.gotHeaders
	c.mu.Unlock()
	if timer != nil {
		timer.Stop()
	}
	if !gotHeaders {
		s.finish()
		return err
	}
	resp.SetStatus(s.status, 2, 0)
	resp.SetBody(s, s.contentLen)
	return nil
}

func (c *ClientConn) writeRequestHeaders(id uint32, req *exchange.ClientRequest, endStream bool) error {
	b := c.enc.startBlock(c.wbuf[:0])
	b = c.enc.appendField(b, nameMethod, bytesconv.S2B(req.Method))
	b = c.enc.appendField(b, nameScheme, bytesconv.S2B(req.Scheme))
	b = c.enc.appendField(b, nameAuthority, req.Authority)
	b = c.enc.appendField(b, namePath, req.Target)
	for i := range req.Header.Len() {
		name, value := req.Header.At(i)
		switch string(name) {
		case "host", "connection", "keep-alive", "proxy-connection", "transfer-encoding", "upgrade",
			"content-length":
			continue // connection-specific fields are not allowed in HTTP/2 (RFC 9113 section 8.2.2)
		case "te":
			if !equalFoldBytes(value, "trailers") {
				continue
			}
		}
		b = c.enc.appendField(b, name, value)
	}
	if req.Body != nil && req.ContentLength >= 0 {
		var n [20]byte
		b = c.enc.appendField(b, nameContentLength, strconv.AppendInt(n[:0], req.ContentLength, 10))
	}
	c.wbuf = b
	maxFrame := int(c.peerFrameSize())
	typ, flags := frameHeaders, uint8(0)
	if endStream {
		flags = flagEndStream
	}
	for first := true; first || len(b) > 0; first = false {
		n := min(len(b), maxFrame)
		f := flags
		if n == len(b) {
			f |= flagEndHeaders
		}
		_, _ = c.bw.Write(appendFrameHeader(c.fhdr[:0], n, typ, f, id))
		_, _ = c.bw.Write(b[:n])
		b = b[n:]
		typ, flags = frameContinuation, 0
	}
	return c.bw.Flush()
}

var (
	nameMethod        = []byte(":method")
	nameScheme        = []byte(":scheme")
	nameAuthority     = []byte(":authority")
	namePath          = []byte(":path")
	nameContentLength = []byte("content-length")
)

func equalFoldBytes(b []byte, lower string) bool {
	if len(b) != len(lower) {
		return false
	}
	for i, ch := range b {
		if 'A' <= ch && ch <= 'Z' {
			ch += 'a' - 'A'
		}
		if ch != lower[i] {
			return false
		}
	}
	return true
}

func (c *ClientConn) peerFrameSize() uint32 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.peerFrame
}

// writeBody sends the request body as DATA frames within the send windows.
func (s *clientStream) writeBody(body io.Reader) error {
	c := s.c
	buf := make([]byte, minMaxFrameSize)
	for {
		n, rerr := body.Read(buf)
		p := buf[:n]
		end := errors.Is(rerr, io.EOF)
		if rerr != nil && !end {
			return rerr
		}
		for first := true; first || len(p) > 0; first = false {
			c.mu.Lock()
			for len(p) > 0 && (s.sendWindow <= 0 || c.sendWindow <= 0) && s.err == nil && !c.closed && !s.sentEnd {
				c.flow.Wait()
			}
			if s.sentEnd { // the server already answered and needs no more of the body
				c.mu.Unlock()
				return nil
			}
			if err := s.err; err != nil || c.closed {
				c.mu.Unlock()
				if err == nil {
					err = errClientConnClosed
				}
				return err
			}
			m := min(int64(len(p)), s.sendWindow, c.sendWindow, int64(c.peerFrame))
			s.sendWindow -= m
			c.sendWindow -= m
			c.mu.Unlock()
			last := end && int(m) == len(p)
			if err := c.writeData(s.id, p[:m], last); err != nil {
				return err
			}
			p = p[m:]
			if last {
				c.mu.Lock()
				s.sentEnd = true
				c.mu.Unlock()
				return nil
			}
		}
		if end {
			return nil
		}
	}
}

func (c *ClientConn) writeData(id uint32, p []byte, end bool) error {
	var flags uint8
	if end {
		flags = flagEndStream
	}
	c.wmu.Lock()
	defer c.wmu.Unlock()
	_, _ = c.bw.Write(appendFrameHeader(c.fhdr[:0], len(p), frameData, flags, id))
	_, _ = c.bw.Write(p)
	return c.bw.Flush()
}

func (c *ClientConn) writeFrame(typ frameType, flags uint8, id uint32, payload []byte) {
	c.wmu.Lock()
	defer c.wmu.Unlock()
	_, _ = c.bw.Write(appendFrameHeader(c.fhdr[:0], len(payload), typ, flags, id))
	_, _ = c.bw.Write(payload)
	if err := c.bw.Flush(); err != nil {
		go c.shutdown(err)
	}
}

// abort ends the stream with err, telling the server to stop with RST_STREAM if it has not finished.
func (s *clientStream) abort(err error) {
	c := s.c
	c.mu.Lock()
	if s.err != nil || s.done || s.endStream && s.sentEnd {
		c.mu.Unlock()
		return
	}
	s.err = err
	s.cond.Broadcast()
	c.flow.Broadcast()
	closed := c.closed
	c.mu.Unlock()
	if !closed {
		var code [4]byte
		c.writeFrame(frameRSTStream, 0, s.id, appendBE32(code[:0], uint32(ErrCodeCancel)))
	}
}

// Read reads the response body.
func (s *clientStream) Read(p []byte) (int, error) {
	c := s.c
	c.mu.Lock()
	for s.off == len(s.buf) && !s.endStream && s.err == nil {
		s.cond.Wait()
	}
	if s.off < len(s.buf) {
		n := copy(p, s.buf[s.off:])
		s.off += n
		if s.off == len(s.buf) {
			s.buf, s.off = s.buf[:0], 0
		}
		s.unacked += uint32(n)
		update := uint32(0)
		if s.unacked >= c.cfg.StreamWindow/2 && !s.endStream {
			update, s.unacked = s.unacked, 0
		}
		c.mu.Unlock()
		if update > 0 {
			var b [4]byte
			c.writeFrame(frameWindowUpdate, 0, s.id, appendBE32(b[:0], update))
		}
		return n, nil
	}
	err := s.err
	c.mu.Unlock()
	if err == nil {
		err = io.EOF
	}
	s.finish()
	return 0, err
}

// Close abandons the rest of the body.
func (s *clientStream) Close() error {
	s.abort(errBodyAfterClose)
	s.finish()
	return nil
}

// finish removes the stream from the connection once its caller is done with it.
func (s *clientStream) finish() {
	c := s.c
	c.mu.Lock()
	if s.done {
		c.mu.Unlock()
		return
	}
	s.done = true
	delete(c.streams, s.id)
	c.active--
	if c.active == 0 {
		c.lastActive = time.Now()
	}
	// An upload still running is not wanted once the response is over; stop it and tell the server.
	stopUpload := !s.sentEnd && s.err == nil && !c.closed
	if stopUpload {
		s.err = errBodyAfterClose
	}
	c.flow.Broadcast()
	c.mu.Unlock()
	if stopUpload {
		var code [4]byte
		c.writeFrame(frameRSTStream, 0, s.id, appendBE32(code[:0], uint32(ErrCodeCancel)))
	}
	if s.writing != nil {
		<-s.writing // the caller's body is never read after the response is released
	}
	if s.stopCancel != nil {
		s.stopCancel()
	}
}

// readLoop reads frames until the connection fails, then fails every stream.
func (c *ClientConn) readLoop() {
	err := c.readFrames()
	c.mu.Lock()
	if c.err == nil {
		c.err = err
	}
	if c.err == nil || errors.Is(c.err, io.EOF) {
		c.err = errClientConnClosed
	}
	c.closed = true
	for _, s := range c.streams {
		if s.err == nil && (!s.endStream || !s.gotHeaders) {
			s.err = c.err
		}
		s.cond.Broadcast()
	}
	c.flow.Broadcast()
	c.mu.Unlock()
	_ = c.nc.Close()
	c.onClose(c)
}

func (c *ClientConn) readFrames() error {
	for {
		h, p, err := c.fr.readFrame()
		if err != nil {
			return err
		}
		if c.blockID != 0 && h.typ != frameContinuation {
			return connError{ErrCodeProtocol, "header block interrupted"}
		}
		switch h.typ {
		case frameData:
			err = c.onData(h, p)
		case frameHeaders:
			err = c.onHeaders(h, p)
		case frameContinuation:
			if h.streamID != c.blockID || c.blockID == 0 {
				return connError{ErrCodeProtocol, "unexpected CONTINUATION"}
			}
			err = c.onHeaderFragment(h, p)
		case frameRSTStream:
			err = c.onReset(h, p)
		case frameSettings:
			err = c.onSettings(h, p)
		case framePing:
			if len(p) != 8 || h.streamID != 0 {
				return connError{ErrCodeFrameSize, "bad PING"}
			}
			if !h.has(flagAck) {
				c.writeFrame(framePing, flagAck, 0, p)
			}
		case frameGoAway:
			err = c.onGoAway(h, p)
		case frameWindowUpdate:
			err = c.onWindowUpdate(h, p)
		case framePushPromise:
			return connError{ErrCodeProtocol, "PUSH_PROMISE with push disabled"}
		}
		if err != nil {
			var ce connError
			if errors.As(err, &ce) {
				var b []byte
				b = appendBE32(b, 0)
				b = appendBE32(b, uint32(ce.code))
				c.writeFrame(frameGoAway, 0, 0, b)
			}
			return err
		}
	}
}

func (c *ClientConn) onData(h frameHeader, p []byte) error {
	data, _, err := stripPadding(h, p)
	if err != nil {
		return err
	}
	// The whole frame, padding included, counts against flow control. Streams limit buffering, so the
	// connection window is returned in large batches.
	c.mu.Lock()
	c.connUnacked += int64(len(p))
	if c.connUnacked >= clientConnWindow/2 {
		inc := c.connUnacked
		c.connUnacked = 0
		c.mu.Unlock()
		var b [4]byte
		c.writeFrame(frameWindowUpdate, 0, 0, appendBE32(b[:0], uint32(inc)))
		c.mu.Lock()
	}
	s := c.streams[h.streamID]
	if s == nil || s.err != nil || !s.gotHeaders || s.endStream {
		c.mu.Unlock()
		if s != nil && !s.gotHeaders {
			return connError{ErrCodeProtocol, "DATA before HEADERS"}
		}
		return nil
	}
	if s.head {
		s.err = errMalformedResp
	} else {
		if len(s.buf)-s.off+len(data) > int(c.cfg.StreamWindow) {
			c.mu.Unlock()
			return connError{ErrCodeFlowControl, "stream window exceeded"}
		}
		s.buf = append(s.buf, data...)
	}
	if h.has(flagEndStream) {
		s.endStream = true
	}
	s.cond.Broadcast()
	c.mu.Unlock()
	return nil
}

func (c *ClientConn) onHeaders(h frameHeader, p []byte) error {
	frag, _, err := stripPadding(h, p)
	if err != nil {
		return err
	}
	if h.has(flagPriority) {
		if len(frag) < 5 {
			return connError{ErrCodeFrameSize, "HEADERS too short for priority"}
		}
		frag = frag[5:]
	}
	if h.streamID == 0 || h.streamID%2 == 0 {
		return connError{ErrCodeProtocol, "HEADERS on a server stream"}
	}
	c.blockID, c.blockN, c.blockES = h.streamID, 0, h.has(flagEndStream)
	c.fields = c.fields[:0]
	return c.onHeaderFragment(h, frag)
}

func (c *ClientConn) onHeaderFragment(h frameHeader, frag []byte) error {
	if c.blockN++; c.blockN > maxBlockFrames {
		return connError{ErrCodeEnhanceYourCalm, "too many CONTINUATION frames"}
	}
	if _, err := c.dec.Write(frag); err != nil {
		return connError{ErrCodeCompression, err.Error()}
	}
	if !h.has(flagEndHeaders) {
		return nil
	}
	id := c.blockID
	c.blockID = 0
	if err := c.dec.Close(); err != nil {
		return connError{ErrCodeCompression, err.Error()}
	}
	return c.endHeaderBlock(id)
}

// endHeaderBlock applies a decoded header block to its stream: the response header, an informational
// response to skip, or trailers.
func (c *ClientConn) endHeaderBlock(id uint32) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	s := c.streams[id]
	if s == nil || s.err != nil || s.done {
		return nil
	}
	if !s.gotHeaders {
		status, err := s.applyHeader(c.fields)
		if err != nil {
			s.fail(err)
			c.resetLocked(id, ErrCodeProtocol)
			return nil
		}
		if status < 200 {
			if c.blockES {
				s.fail(errMalformedResp)
			}
			return nil
		}
		s.status = status
		s.gotHeaders = true
		if c.blockES || s.head || status == 204 || status == 304 {
			s.endStream = true
		}
		s.cond.Broadcast()
		return nil
	}
	if !c.blockES {
		s.fail(errMalformedResp)
		c.resetLocked(id, ErrCodeProtocol)
		return nil
	}
	for _, f := range c.fields {
		if len(f.Name) > 0 && f.Name[0] == ':' {
			s.fail(errMalformedResp)
			return nil
		}
		if s.resp.AddTrailer(bytesconv.S2B(f.Name), bytesconv.S2B(f.Value)) != nil {
			s.fail(errMalformedResp)
			return nil
		}
	}
	s.endStream = true
	s.cond.Broadcast()
	return nil
}

// applyHeader validates a response header block and copies its fields to the response.
// Callers hold c.mu, so the caller waiting in RoundTrip does not see a partial header.
func (s *clientStream) applyHeader(fields []hpack.HeaderField) (int, error) {
	if len(fields) == 0 || fields[0].Name != ":status" || len(fields[0].Value) != 3 {
		return 0, errMalformedResp
	}
	status, err := strconv.Atoi(fields[0].Value)
	if err != nil || status < 100 {
		return 0, errMalformedResp
	}
	if status < 200 {
		return status, nil
	}
	for _, f := range fields[1:] {
		if len(f.Name) == 0 || f.Name[0] == ':' {
			return 0, errMalformedResp
		}
		if f.Name == "content-length" {
			n, ok := bytesconv.ParseUint(bytesconv.S2B(f.Value))
			if !ok {
				return 0, errMalformedResp
			}
			s.contentLen = n
		}
		if err := s.resp.AddHeader(bytesconv.S2B(f.Name), bytesconv.S2B(f.Value)); err != nil {
			return 0, err
		}
	}
	if s.head {
		s.contentLen = max(s.contentLen, 0)
		if status != 200 {
			s.contentLen = 0
		}
	}
	return status, nil
}

func (s *clientStream) fail(err error) {
	if s.err == nil {
		s.err = err
	}
	s.cond.Broadcast()
	s.c.flow.Broadcast()
}

// resetLocked sends RST_STREAM. Callers hold c.mu, so the frame is written from a new goroutine.
func (c *ClientConn) resetLocked(id uint32, code ErrCode) {
	go func() {
		var b [4]byte
		c.writeFrame(frameRSTStream, 0, id, appendBE32(b[:0], uint32(code)))
	}()
}

func (c *ClientConn) onReset(h frameHeader, p []byte) error {
	if len(p) != 4 || h.streamID == 0 {
		return connError{ErrCodeFrameSize, "bad RST_STREAM"}
	}
	code := ErrCode(be32(p))
	c.mu.Lock()
	defer c.mu.Unlock()
	s := c.streams[h.streamID]
	if s == nil {
		return nil
	}
	switch {
	case code == ErrCodeRefusedStream:
		s.fail(exchange.NotProcessed(errStreamRefused))
	case code == ErrCodeNo && s.gotHeaders && s.endStream:
		// The server has its whole response and does not need the rest of our body (RFC 9113 section 8.1).
		s.sentEnd = true
		c.flow.Broadcast()
	default:
		s.fail(streamError{h.streamID, code, "reset by server"})
	}
	return nil
}

func (c *ClientConn) onSettings(h frameHeader, p []byte) error {
	if h.streamID != 0 {
		return connError{ErrCodeProtocol, "SETTINGS on a stream"}
	}
	if h.has(flagAck) {
		return nil
	}
	if len(p)%6 != 0 {
		return connError{ErrCodeFrameSize, "SETTINGS length"}
	}
	tableSize := int64(-1)
	c.mu.Lock()
	for ; len(p) > 0; p = p[6:] {
		id, v := settingID(uint16(p[0])<<8|uint16(p[1])), be32(p[2:])
		switch id {
		case settingMaxConcurrentStreams:
			c.maxStreams = v
		case settingInitialWindowSize:
			if v > maxWindow {
				c.mu.Unlock()
				return connError{ErrCodeFlowControl, "initial window too large"}
			}
			delta := int64(v) - c.peerWindow
			c.peerWindow = int64(v)
			for _, s := range c.streams {
				s.sendWindow += delta
			}
		case settingMaxFrameSize:
			if v < minMaxFrameSize || v > maxMaxFrameSize {
				c.mu.Unlock()
				return connError{ErrCodeProtocol, "invalid SETTINGS_MAX_FRAME_SIZE"}
			}
			c.peerFrame = v
		case settingHeaderTableSize:
			tableSize = int64(v)
		}
	}
	c.flow.Broadcast()
	c.mu.Unlock()
	// The write lock is taken before c.mu elsewhere, so never while holding it.
	if tableSize >= 0 {
		c.wmu.Lock()
		c.enc.setMaxSize(uint32(tableSize))
		c.wmu.Unlock()
	}
	c.writeFrame(frameSettings, flagAck, 0, nil)
	return nil
}

func (c *ClientConn) onGoAway(h frameHeader, p []byte) error {
	if len(p) < 8 || h.streamID != 0 {
		return connError{ErrCodeFrameSize, "bad GOAWAY"}
	}
	last := be32(p) & maxStreamID
	c.mu.Lock()
	c.goAway = true
	for id, s := range c.streams {
		// Streams above the last one the server processed were never acted on and can be retried elsewhere.
		if id > last {
			s.fail(exchange.NotProcessed(errStreamRefused))
		}
	}
	c.mu.Unlock()
	return nil
}

func (c *ClientConn) onWindowUpdate(h frameHeader, p []byte) error {
	if len(p) != 4 {
		return connError{ErrCodeFrameSize, "bad WINDOW_UPDATE"}
	}
	inc := int64(be32(p) & maxWindow)
	c.mu.Lock()
	defer c.mu.Unlock()
	if h.streamID == 0 {
		if c.sendWindow += inc; c.sendWindow > maxWindow {
			return connError{ErrCodeFlowControl, "connection window overflow"}
		}
	} else if s := c.streams[h.streamID]; s != nil {
		if s.sendWindow += inc; s.sendWindow > maxWindow {
			s.fail(streamError{h.streamID, ErrCodeFlowControl, "stream window overflow"})
		}
	}
	c.flow.Broadcast()
	return nil
}
