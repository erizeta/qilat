package h3

import (
	"bufio"
	"context"
	"crypto/tls"
	"errors"
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/erizeta/qilat/internal/exchange"
	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/quicvarint"
)

// Config holds the server settings for HTTP/3 connections, with defaults already resolved by the core.
type Config struct {
	// Acquire returns a fresh exchange for each request stream.
	Acquire              func() exchange.Exchange
	MaxFieldSectionSize  int
	MaxRequestBodySize   int64 // -1 means no limit
	MaxConcurrentStreams int   // handlers running at once per connection
	ReadHeaderTimeout    time.Duration
	ReadTimeout          time.Duration
	WriteTimeout         time.Duration
	ReadStallTimeout     time.Duration
	WriteStallTimeout    time.Duration
	// Allow0RTT is set when the QUIC listener accepts 0-RTT; requests other than GET, HEAD, and OPTIONS that
	// arrive before the handshake completes are then answered 425 Too Early (RFC 8470).
	Allow0RTT bool
	Logger    *slog.Logger
}

// Listener accepts HTTP/3 connections.
type Listener struct {
	ln  *quic.EarlyListener
	cfg *Config
}

// QUICConfig holds the QUIC transport limits the core passes through.
type QUICConfig struct {
	MaxIncomingStreams         int64
	MaxIdleTimeout             time.Duration
	HandshakeIdleTimeout       time.Duration
	MaxStreamReceiveWindow     uint64
	MaxConnectionReceiveWindow uint64
	Allow0RTT                  bool
}

// Listen starts accepting QUIC connections on pc. tlsConf must hold certificates; it is cloned and
// restricted to TLS 1.3 with ALPN "h3".
func Listen(pc net.PacketConn, tlsConf *tls.Config, qc QUICConfig, cfg *Config) (*Listener, error) {
	tc := tlsConf.Clone()
	tc.MinVersion = tls.VersionTLS13
	tc.NextProtos = []string{"h3"}
	ln, err := quic.ListenEarly(pc, tc, &quic.Config{
		MaxIncomingStreams:         qc.MaxIncomingStreams,
		MaxIncomingUniStreams:      16, // control and QPACK streams, plus room for unknown types
		MaxIdleTimeout:             qc.MaxIdleTimeout,
		HandshakeIdleTimeout:       qc.HandshakeIdleTimeout,
		MaxStreamReceiveWindow:     qc.MaxStreamReceiveWindow,
		MaxConnectionReceiveWindow: qc.MaxConnectionReceiveWindow,
		Allow0RTT:                  qc.Allow0RTT,
	})
	if err != nil {
		return nil, err
	}
	return &Listener{ln: ln, cfg: cfg}, nil
}

// Accept waits for the next connection.
func (l *Listener) Accept(ctx context.Context) (*Conn, error) {
	qc, err := l.ln.Accept(ctx)
	if err != nil {
		return nil, err
	}
	return newConn(qc, l.cfg), nil
}

// Addr returns the UDP address the listener serves.
func (l *Listener) Addr() net.Addr { return l.ln.Addr() }

// Close stops accepting connections. Established connections are unaffected.
func (l *Listener) Close() error { return l.ln.Close() }

// Conn serves HTTP/3 on one QUIC connection.
type Conn struct {
	qc  *quic.Conn
	cfg *Config
	tls atomic.Pointer[tls.ConnectionState]
	sem chan struct{}

	ctrlMu sync.Mutex // serializes writes to ctrl
	ctrl   *quic.SendStream

	mu        sync.Mutex
	active    int
	nextID    quic.StreamID // lowest client stream ID not yet accepted
	goAway    bool
	goAwayID  quic.StreamID
	peerCtrl  bool
	peerQPACK [2]bool
	draining  bool // a close is scheduled after GOAWAY
	wg        sync.WaitGroup
}

func newConn(qc *quic.Conn, cfg *Config) *Conn {
	return &Conn{qc: qc, cfg: cfg, sem: make(chan struct{}, max(cfg.MaxConcurrentStreams, 1))}
}

// Serve runs the connection until it closes. It returns after every handler has finished.
func (c *Conn) Serve() {
	defer c.wg.Wait()
	ctx := c.qc.Context()
	go func() {
		select {
		case <-c.qc.HandshakeComplete():
			st := c.qc.ConnectionState().TLS
			c.tls.Store(&st)
		case <-ctx.Done():
		}
	}()
	if err := c.openControl(); err != nil {
		c.close(errClosedCriticalStream, "cannot open control stream")
		return
	}
	go c.acceptUni(ctx)
	for {
		select {
		case c.sem <- struct{}{}: // at most MaxConcurrentStreams handlers, even for streams the client resets
		case <-ctx.Done():
			return
		}
		str, err := c.qc.AcceptStream(ctx)
		if err != nil {
			<-c.sem
			return
		}
		c.mu.Lock()
		rejected := c.goAway && str.StreamID() >= c.goAwayID
		if !rejected {
			c.active++
			c.nextID = max(c.nextID, str.StreamID()+4)
		}
		c.mu.Unlock()
		if rejected {
			<-c.sem
			str.CancelRead(errRequestRejected)
			str.CancelWrite(errRequestRejected)
			continue
		}
		c.wg.Add(1)
		go c.serveStream(str)
	}
}

func (c *Conn) openControl() error {
	ctrl, err := c.qc.OpenUniStream()
	if err != nil {
		return err
	}
	c.ctrl = ctrl
	b := quicvarint.Append(nil, streamControl)
	var settings []byte
	settings = quicvarint.Append(settings, settingQPACKMaxTableCapacity)
	settings = quicvarint.Append(settings, 0)
	settings = quicvarint.Append(settings, settingQPACKBlockedStreams)
	settings = quicvarint.Append(settings, 0)
	settings = quicvarint.Append(settings, settingMaxFieldSectionSize)
	settings = quicvarint.Append(settings, uint64(c.cfg.MaxFieldSectionSize))
	b = appendFrameHeader(b, frameSettings, uint64(len(settings)))
	_, err = ctrl.Write(append(b, settings...))
	return err
}

// close ends the connection with an HTTP/3 error code.
func (c *Conn) close(code quic.ApplicationErrorCode, reason string) {
	if c.cfg.Logger != nil && code != errNoError {
		c.cfg.Logger.Debug("qilat: HTTP/3 connection error", "remote", c.qc.RemoteAddr(), "code", code, "reason", reason)
	}
	_ = c.qc.CloseWithError(code, reason)
}

func (c *Conn) closeOnError(err error) {
	var ce *connError
	if errors.As(err, &ce) {
		c.close(ce.code, ce.reason)
	}
}

// CloseIfIdle starts a graceful shutdown with GOAWAY; the connection closes itself shortly after its last
// request finishes. It reports whether the connection is already closed.
func (c *Conn) CloseIfIdle() bool {
	select {
	case <-c.qc.Context().Done():
		return true
	default:
	}
	c.mu.Lock()
	send := !c.goAway
	c.goAway, c.goAwayID = true, c.nextID
	idle := c.active == 0
	id := c.goAwayID
	c.mu.Unlock()
	if send {
		c.writeGoAway(id)
	}
	if idle {
		c.drain()
	}
	return false
}

// drain closes the connection once responses already written have had time to reach the client: closing a
// QUIC connection discards data still in flight. The client usually closes first after GOAWAY.
func (c *Conn) drain() {
	c.mu.Lock()
	started := c.draining
	c.draining = true
	c.mu.Unlock()
	if started {
		return
	}
	wait := min(max(4*c.qc.ConnectionStats().SmoothedRTT, 100*time.Millisecond), 2*time.Second)
	time.AfterFunc(wait, func() { c.close(errNoError, "") })
}

// Close closes the connection immediately.
func (c *Conn) Close() { c.close(errNoError, "") }

func (c *Conn) writeGoAway(id quic.StreamID) {
	payload := quicvarint.Append(nil, uint64(id))
	b := appendFrameHeader(nil, frameGoAway, uint64(len(payload)))
	c.ctrlMu.Lock()
	_, _ = c.ctrl.Write(append(b, payload...))
	c.ctrlMu.Unlock()
}

func (c *Conn) streamDone() {
	c.mu.Lock()
	c.active--
	closeNow := c.goAway && c.active == 0
	c.mu.Unlock()
	<-c.sem
	if closeNow {
		c.drain()
	}
}

func (c *Conn) acceptUni(ctx context.Context) {
	for {
		rs, err := c.qc.AcceptUniStream(ctx)
		if err != nil {
			return
		}
		go c.serveUni(rs)
	}
}

func (c *Conn) serveUni(rs *quic.ReceiveStream) {
	br := bufio.NewReader(rs)
	typ, err := quicvarint.Read(br)
	if err != nil {
		return
	}
	c.mu.Lock()
	dup := false
	switch typ {
	case streamControl:
		dup, c.peerCtrl = c.peerCtrl, true
	case streamQPACKEncoder, streamQPACKDecoder:
		i := typ - streamQPACKEncoder
		dup, c.peerQPACK[i] = c.peerQPACK[i], true
	}
	c.mu.Unlock()
	if dup {
		c.close(errStreamCreation, "duplicate critical stream")
		return
	}
	switch typ {
	case streamControl:
		err = c.readControl(br)
	case streamPush:
		err = newConnError(errStreamCreation, "push stream from client")
	case streamQPACKEncoder:
		err = readEncoderStream(br)
	case streamQPACKDecoder:
		// With no dynamic table there is nothing to acknowledge; instructions are ignored.
		_, err = io.Copy(io.Discard, br)
		if err == nil {
			err = io.EOF
		}
	default:
		rs.CancelRead(errStreamCreation) // unknown stream types are ignored (RFC 9114 section 6.2)
		return
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		err = newConnError(errClosedCriticalStream, "critical stream closed")
	}
	select {
	case <-c.qc.Context().Done():
		return // the connection is already going away
	default:
	}
	c.closeOnError(err)
}

func (c *Conn) readControl(br *bufio.Reader) error {
	typ, length, err := readFrameHeader(br)
	if err != nil {
		return err
	}
	if typ != frameSettings {
		return newConnError(errMissingSettings, "first control frame is not SETTINGS")
	}
	if err := readSettings(br, length); err != nil {
		return err
	}
	for {
		typ, length, err := readFrameHeader(br)
		if err != nil {
			return err
		}
		switch {
		case typ == frameSettings, typ == frameData, typ == frameHeaders, typ == framePushPromise, isHTTP2Frame(typ):
			return newConnError(errFrameUnexpected, "unexpected frame on control stream")
		case typ == frameGoAway, typ == frameMaxPushID, typ == frameCancelPush:
			// The client's GOAWAY and push frames need no action: the server neither pushes nor opens requests.
			if length > 8 {
				return newConnError(errFrame, "oversized control frame")
			}
		}
		if err := skip(br, length); err != nil {
			return err
		}
	}
}

func readSettings(br *bufio.Reader, length uint64) error {
	if length > 1<<12 {
		return newConnError(errExcessiveLoad, "SETTINGS too large")
	}
	p := make([]byte, length)
	if _, err := io.ReadFull(br, p); err != nil {
		return err
	}
	var seen [8]bool
	for len(p) > 0 {
		id, n, err := quicvarint.Parse(p)
		if err != nil {
			return newConnError(errFrame, "malformed SETTINGS")
		}
		p = p[n:]
		if _, n, err = quicvarint.Parse(p); err != nil {
			return newConnError(errFrame, "malformed SETTINGS")
		}
		p = p[n:]
		switch {
		case id >= 0x2 && id <= 0x5:
			return newConnError(errSettings, "HTTP/2 setting in HTTP/3")
		case id < uint64(len(seen)):
			if seen[id] {
				return newConnError(errSettings, "duplicate setting")
			}
			seen[id] = true
		}
		// The client's limits concern its own decoder; responses never use the dynamic table.
	}
	return nil
}

// readEncoderStream accepts only "Set Dynamic Table Capacity 0": the advertised capacity is 0, so any insertion
// is an error (RFC 9204 section 4.3).
func readEncoderStream(br *bufio.Reader) error {
	for {
		b, err := br.ReadByte()
		if err != nil {
			return err
		}
		if b != 0x20 {
			return newConnError(errQPACKEncoderStream, "dynamic table instruction with capacity 0")
		}
	}
}

func (c *Conn) tlsState() *tls.ConnectionState {
	if st := c.tls.Load(); st != nil {
		return st
	}
	st := c.qc.ConnectionState().TLS
	return &st
}

// handshakeDone reports whether the handshake has completed; requests before that arrived in 0-RTT.
func (c *Conn) handshakeDone() bool {
	select {
	case <-c.qc.HandshakeComplete():
		return true
	default:
		return false
	}
}
