package h2

import (
	"bufio"
	"crypto/tls"
	"errors"
	"io"
	"log/slog"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/erizeta/qilat/internal/bytesconv"
	"github.com/erizeta/qilat/internal/exchange"
	"github.com/erizeta/qilat/internal/httpfields"
	"golang.org/x/net/http2/hpack"
)

// Config holds the server settings for HTTP/2 connections. The core resolves defaults before building it:
// zero durations mean no limit and a negative MaxRequestBodySize means no limit.
type Config struct {
	// Acquire returns a fresh exchange for each stream.
	Acquire              func() exchange.Exchange
	MaxConcurrentStreams uint32
	MaxReadFrameSize     uint32
	MaxHeaderListSize    uint32
	HeaderTableSize      uint32
	// StreamWindow and ConnWindow are the receive buffers per stream and per connection, at least 65535.
	StreamWindow       uint32
	ConnWindow         uint32
	MaxRequestBodySize int64
	ReadHeaderTimeout  time.Duration
	ReadTimeout        time.Duration
	WriteTimeout       time.Duration
	IdleTimeout        time.Duration
	Logger             *slog.Logger
}

const (
	writeBufferSize = 16 << 10
	// calmBurst and calmRate bound frames that cost work but carry no request (PING, SETTINGS, RST_STREAM,
	// PRIORITY, empty DATA, unknown types). A peer exceeding them gets GOAWAY ENHANCE_YOUR_CALM.
	calmBurst = 1000
	calmRate  = 1000 // per second
	// maxBlockFrames bounds CONTINUATION frames per header block (CVE-2024-27316 style floods).
	maxBlockFrames = 128
	// recentResets is how many streams we reset are remembered, so frames still in flight for them are ignored.
	recentResets = 32
)

var (
	readerPool sync.Pool
	writerPool sync.Pool
)

// Conn serves HTTP/2 on one connection.
type Conn struct {
	nc       net.Conn
	cfg      *Config
	remote   net.Addr
	tlsState *tls.ConnectionState
	br       *bufio.Reader
	ownsBR   bool
	fr       framer
	dec      *hpack.Decoder
	hb       headerBlock // reader goroutine only
	calm     float64     // reader goroutine only
	calmAt   time.Time   // reader goroutine only
	resets   [recentResets]uint32
	resetPos int

	sched      writeSched
	bw         *bufio.Writer        // guarded by sched
	wbuf       []byte               // guarded by sched
	fhdr       [frameHeaderLen]byte // guarded by sched; a local array would escape through bufio
	needFlush  bool                 // guarded by sched
	enc        *encoder             // guarded by sched
	writeErr   error                // guarded by sched
	ctrl       *waiter              // the reader's waiter
	ext        *waiter              // used by CloseIfIdle
	peerFrame  atomic.Uint32        // peer's SETTINGS_MAX_FRAME_SIZE
	started    atomic.Bool          // the server preface has been sent
	closedConn atomic.Bool

	mu          sync.Mutex
	flow        sync.Cond // signaled when send windows grow, streams reset, or the connection closes
	streams     map[uint32]*stream
	open        uint32 // streams counted toward MaxConcurrentStreams
	maxClientID uint32
	sendWindow  int64
	peerWindow  int64 // peer's SETTINGS_INITIAL_WINDOW_SIZE
	recvWindow  int64
	recvUnacked int64
	running     int
	pending     []*stream
	goAway      bool
	goAwayID    uint32
	closed      bool
	idleSet     bool
	wg          sync.WaitGroup
}

// NewConn prepares nc for HTTP/2. br holds bytes already read from nc, such as a sniffed preface; nil means none.
func NewConn(nc net.Conn, br *bufio.Reader, tlsState *tls.ConnectionState, cfg *Config) *Conn {
	c := &Conn{nc: nc, cfg: cfg, remote: nc.RemoteAddr(), tlsState: tlsState, br: br}
	c.flow.L = &c.mu
	c.streams = make(map[uint32]*stream)
	c.ctrl = newWaiter(0, nil)
	c.enc = newEncoder(defaultHeaderTable)
	c.ext = newWaiter(0, nil)
	c.sendWindow = defaultWindow
	c.peerWindow = defaultWindow
	c.peerFrame.Store(minMaxFrameSize)
	c.recvWindow = int64(cfg.ConnWindow)
	return c
}

// CloseIfIdle starts a graceful shutdown with GOAWAY and closes the connection once no stream is active.
// It reports whether the connection is closed.
func (c *Conn) CloseIfIdle() bool {
	if !c.started.Load() {
		c.Close()
		return true
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return true
	}
	send := !c.goAway
	c.goAway, c.goAwayID = true, c.maxClientID
	idle := len(c.streams) == 0
	last := c.goAwayID
	c.mu.Unlock()
	if send {
		_ = c.writeControl(c.ext, func(b []byte) []byte { return appendGoAway(b, last, ErrCodeNo) })
	}
	if idle {
		c.Close()
	}
	return idle
}

// Close closes the connection immediately; running handlers see their streams reset.
func (c *Conn) Close() {
	if c.closedConn.CompareAndSwap(false, true) {
		_ = c.nc.Close()
	}
}

// Serve runs the connection until it closes. It returns after every handler has finished.
func (c *Conn) Serve() {
	defer c.finish()
	if c.br == nil {
		c.br = getReader(c.nc)
		c.ownsBR = true
	}
	c.fr = framer{r: c.br, maxRead: c.cfg.MaxReadFrameSize}
	// Strings up to twice the list limit are decoded so an oversized header gets 431; longer ones would make
	// the decoder stop mid-block, losing HPACK sync, so they end the connection.
	c.dec = newDecoder(c.cfg.HeaderTableSize, int(min(uint64(c.cfg.MaxHeaderListSize)*2, 1<<30)), c.onField)

	if d := c.cfg.ReadHeaderTimeout; d > 0 {
		_ = c.nc.SetReadDeadline(time.Now().Add(d))
	}
	var preface [len(ClientPreface)]byte
	if _, err := io.ReadFull(c.br, preface[:]); err != nil {
		return
	}
	if string(preface[:]) != ClientPreface {
		if c.writeControl(c.ctrl, func(b []byte) []byte { return appendGoAway(b, 0, ErrCodeProtocol) }) == nil {
			c.lingerAfterGoAway()
		}
		return
	}
	if c.writeControl(c.ctrl, c.appendServerPreface) != nil {
		return
	}
	c.started.Store(true)

	h, p, err := c.fr.readFrame()
	if err == nil && (h.typ != frameSettings || h.has(flagAck)) {
		err = connError{ErrCodeProtocol, "first frame is not SETTINGS"}
	}
	if err == nil {
		err = c.processFrame(h, p)
	}
	c.mu.Lock()
	c.setIdleDeadlineLocked()
	c.mu.Unlock()
	for err == nil {
		if h, p, err = c.fr.readFrame(); err == nil {
			err = c.processFrame(h, p)
		}
		var se streamError
		if errors.As(err, &se) {
			c.resetStream(se.id, se.code)
			err = nil
		}
	}
	c.handleError(err)
}

func (c *Conn) handleError(err error) {
	var ce connError
	switch {
	case errors.As(err, &ce):
		c.logDebug("qilat: HTTP/2 connection error", err)
		c.mu.Lock()
		c.goAway, c.goAwayID = true, c.maxClientID
		last := c.goAwayID
		c.mu.Unlock()
		if c.writeControl(c.ctrl, func(b []byte) []byte { return appendGoAway(b, last, ce.code) }) == nil {
			c.lingerAfterGoAway()
		}
	case errors.Is(err, os.ErrDeadlineExceeded):
		c.mu.Lock()
		idle := len(c.streams) == 0 && !c.goAway
		c.goAway = true
		last := c.maxClientID
		c.mu.Unlock()
		if idle && c.started.Load() {
			_ = c.writeControl(c.ctrl, func(b []byte) []byte { return appendGoAway(b, last, ErrCodeNo) })
		}
	}
}

const (
	goAwayLinger    = time.Second
	goAwayLingerMax = 1 << 20
)

// lingerAfterGoAway keeps reading for a moment before the connection closes: closing with unread input makes
// TCP send RST, which can destroy the GOAWAY before the client reads it.
func (c *Conn) lingerAfterGoAway() {
	// A flush may have been left to a writer queued behind the GOAWAY, so flush here, and stop all later
	// writes, before the write side is closed.
	c.sched.lock(c.ctrl)
	if c.bw != nil && c.writeErr == nil {
		if err := c.bw.Flush(); err != nil {
			c.writeErr = err
		}
	}
	if c.writeErr == nil {
		c.writeErr = errConnClosed
	}
	if cw, ok := c.nc.(interface{ CloseWrite() error }); ok {
		_ = cw.CloseWrite()
	}
	c.sched.unlock()
	_ = c.nc.SetReadDeadline(time.Now().Add(goAwayLinger))
	_, _ = io.Copy(io.Discard, io.LimitReader(c.br, goAwayLingerMax))
}

func (c *Conn) logDebug(msg string, err error) {
	if c.cfg.Logger != nil {
		c.cfg.Logger.Debug(msg, "remote", c.remote, "error", err)
	}
}

func (c *Conn) appendServerPreface(b []byte) []byte {
	start := len(b)
	b = appendFrameHeader(b, 0, frameSettings, 0, 0)
	b = appendSetting(b, settingMaxConcurrentStreams, c.cfg.MaxConcurrentStreams)
	b = appendSetting(b, settingInitialWindowSize, c.cfg.StreamWindow)
	b = appendSetting(b, settingMaxFrameSize, c.cfg.MaxReadFrameSize)
	b = appendSetting(b, settingMaxHeaderListSize, c.cfg.MaxHeaderListSize)
	if c.cfg.HeaderTableSize != defaultHeaderTable {
		b = appendSetting(b, settingHeaderTableSize, c.cfg.HeaderTableSize)
	}
	b = appendSetting(b, settingNoRFC7540Priorities, 1)
	n := len(b) - start - frameHeaderLen
	b[start], b[start+1], b[start+2] = byte(n>>16), byte(n>>8), byte(n)
	if extra := c.cfg.ConnWindow - defaultWindow; extra > 0 {
		b = appendWindowUpdate(b, 0, extra)
	}
	return b
}

func appendSetting(b []byte, id settingID, v uint32) []byte {
	return appendBE32(append(b, byte(id>>8), byte(id)), v)
}

func appendWindowUpdate(b []byte, id, inc uint32) []byte {
	return appendBE32(appendFrameHeader(b, 4, frameWindowUpdate, 0, id), inc)
}

func appendRST(b []byte, id uint32, code ErrCode) []byte {
	return appendBE32(appendFrameHeader(b, 4, frameRSTStream, 0, id), uint32(code))
}

func appendGoAway(b []byte, last uint32, code ErrCode) []byte {
	return appendBE32(appendBE32(appendFrameHeader(b, 8, frameGoAway, 0, 0), last), uint32(code))
}

// spendCalm charges one unit of the control-frame budget.
func (c *Conn) spendCalm() error {
	now := time.Now()
	if c.calmAt.IsZero() {
		c.calm = calmBurst
	} else {
		c.calm = min(calmBurst, c.calm+now.Sub(c.calmAt).Seconds()*calmRate)
	}
	c.calmAt = now
	if c.calm < 1 {
		return connError{ErrCodeEnhanceYourCalm, "too many control frames"}
	}
	c.calm--
	return nil
}

func (c *Conn) processFrame(h frameHeader, p []byte) error {
	if c.hb.active && (h.typ != frameContinuation || h.streamID != c.hb.id) {
		return connError{ErrCodeProtocol, "expected CONTINUATION"}
	}
	switch h.typ {
	case frameData:
		return c.onData(h, p)
	case frameHeaders:
		return c.onHeaders(h, p)
	case frameContinuation:
		if !c.hb.active {
			return connError{ErrCodeProtocol, "unexpected CONTINUATION"}
		}
		return c.onHeaderFragment(h, p)
	case framePriority:
		return c.onPriority(h, p)
	case frameRSTStream:
		return c.onRSTStream(h, p)
	case frameSettings:
		return c.onSettings(h, p)
	case framePushPromise:
		return connError{ErrCodeProtocol, "PUSH_PROMISE from client"}
	case framePing:
		return c.onPing(h, p)
	case frameGoAway:
		if h.streamID != 0 {
			return connError{ErrCodeProtocol, "GOAWAY on a stream"}
		}
		if len(p) < 8 {
			return connError{ErrCodeFrameSize, "short GOAWAY"}
		}
		return nil // the client opens no more streams; those in flight finish normally
	case frameWindowUpdate:
		return c.onWindowUpdate(h, p)
	case framePriorityUpdate:
		return c.onPriorityUpdate(h, p)
	}
	return c.spendCalm() // unknown frame types are ignored
}

func (c *Conn) onSettings(h frameHeader, p []byte) error {
	if h.streamID != 0 {
		return connError{ErrCodeProtocol, "SETTINGS on a stream"}
	}
	if h.has(flagAck) {
		if len(p) != 0 {
			return connError{ErrCodeFrameSize, "SETTINGS ACK with payload"}
		}
		return nil
	}
	if len(p)%6 != 0 {
		return connError{ErrCodeFrameSize, "SETTINGS length not a multiple of 6"}
	}
	if err := c.spendCalm(); err != nil {
		return err
	}
	for ; len(p) > 0; p = p[6:] {
		id, v := settingID(uint16(p[0])<<8|uint16(p[1])), be32(p[2:])
		switch id {
		case settingEnablePush:
			if v > 1 {
				return connError{ErrCodeProtocol, "invalid SETTINGS_ENABLE_PUSH"}
			}
		case settingInitialWindowSize:
			if v > maxWindow {
				return connError{ErrCodeFlowControl, "SETTINGS_INITIAL_WINDOW_SIZE too large"}
			}
			c.mu.Lock()
			delta := int64(v) - c.peerWindow
			c.peerWindow = int64(v)
			for _, st := range c.streams {
				st.sendWindow += delta
				if st.sendWindow > maxWindow {
					c.mu.Unlock()
					return connError{ErrCodeFlowControl, "stream window overflow"}
				}
			}
			c.flow.Broadcast()
			c.mu.Unlock()
		case settingMaxFrameSize:
			if v < minMaxFrameSize || v > maxMaxFrameSize {
				return connError{ErrCodeProtocol, "invalid SETTINGS_MAX_FRAME_SIZE"}
			}
			c.peerFrame.Store(v)
		case settingHeaderTableSize:
			c.sched.lock(c.ctrl)
			c.enc.setMaxSize(v)
			c.sched.unlock()
		}
	}
	return c.writeControl(c.ctrl, func(b []byte) []byte { return appendFrameHeader(b, 0, frameSettings, flagAck, 0) })
}

func (c *Conn) onPing(h frameHeader, p []byte) error {
	if h.streamID != 0 {
		return connError{ErrCodeProtocol, "PING on a stream"}
	}
	if len(p) != 8 {
		return connError{ErrCodeFrameSize, "PING length is not 8"}
	}
	if h.has(flagAck) {
		return nil
	}
	if err := c.spendCalm(); err != nil {
		return err
	}
	var data [8]byte
	copy(data[:], p)
	return c.writeControl(c.ctrl, func(b []byte) []byte {
		return append(appendFrameHeader(b, 8, framePing, flagAck, 0), data[:]...)
	})
}

func (c *Conn) onPriority(h frameHeader, p []byte) error {
	if h.streamID == 0 {
		return connError{ErrCodeProtocol, "PRIORITY on stream 0"}
	}
	if len(p) != 5 {
		return streamError{h.streamID, ErrCodeFrameSize, "PRIORITY length is not 5"}
	}
	if err := c.spendCalm(); err != nil {
		return err
	}
	if be32(p)&maxStreamID == h.streamID {
		return streamError{h.streamID, ErrCodeProtocol, "stream depends on itself"}
	}
	return nil // RFC 7540 priorities are deprecated (RFC 9113 section 5.3.2); RFC 9218 signals are used instead
}

func (c *Conn) onPriorityUpdate(h frameHeader, p []byte) error {
	if h.streamID != 0 {
		return connError{ErrCodeProtocol, "PRIORITY_UPDATE on a stream"}
	}
	if len(p) < 4 {
		return connError{ErrCodeFrameSize, "short PRIORITY_UPDATE"}
	}
	if err := c.spendCalm(); err != nil {
		return err
	}
	id := be32(p) & maxStreamID
	if id == 0 || id%2 == 0 {
		return connError{ErrCodeProtocol, "PRIORITY_UPDATE for an invalid stream"}
	}
	c.mu.Lock()
	if st := c.streams[id]; st != nil {
		p0 := st.priority.Load()
		st.setPriority(httpfields.ParsePriority(p[4:], p0>>1, p0&1 != 0))
	}
	c.mu.Unlock()
	return nil
}

func (c *Conn) onRSTStream(h frameHeader, p []byte) error {
	if h.streamID == 0 {
		return connError{ErrCodeProtocol, "RST_STREAM on stream 0"}
	}
	if len(p) != 4 {
		return connError{ErrCodeFrameSize, "RST_STREAM length is not 4"}
	}
	c.mu.Lock()
	if h.streamID > c.maxClientID {
		c.mu.Unlock()
		return connError{ErrCodeProtocol, "RST_STREAM on an idle stream"}
	}
	st := c.streams[h.streamID]
	if st != nil && !st.reset {
		c.resetLocked(st)
	}
	c.mu.Unlock()
	return c.spendCalm()
}

func (c *Conn) onWindowUpdate(h frameHeader, p []byte) error {
	if len(p) != 4 {
		return connError{ErrCodeFrameSize, "WINDOW_UPDATE length is not 4"}
	}
	inc := int64(be32(p) & maxWindow)
	c.mu.Lock()
	defer c.mu.Unlock()
	if h.streamID == 0 {
		if inc == 0 {
			return connError{ErrCodeProtocol, "WINDOW_UPDATE with zero increment"}
		}
		if c.sendWindow += inc; c.sendWindow > maxWindow {
			return connError{ErrCodeFlowControl, "connection window overflow"}
		}
		c.flow.Broadcast()
		return nil
	}
	if h.streamID > c.maxClientID {
		return connError{ErrCodeProtocol, "WINDOW_UPDATE on an idle stream"}
	}
	st := c.streams[h.streamID]
	if st == nil || st.reset {
		return nil
	}
	if inc == 0 {
		return streamError{h.streamID, ErrCodeProtocol, "WINDOW_UPDATE with zero increment"}
	}
	if st.sendWindow += inc; st.sendWindow > maxWindow {
		return streamError{h.streamID, ErrCodeFlowControl, "stream window overflow"}
	}
	c.flow.Broadcast()
	return nil
}

func (c *Conn) onData(h frameHeader, p []byte) error {
	id := h.streamID
	if id == 0 {
		return connError{ErrCodeProtocol, "DATA on stream 0"}
	}
	data, pad, err := stripPadding(h, p)
	if err != nil {
		return err
	}
	flowLen := int64(h.length)
	c.mu.Lock()
	if flowLen > c.recvWindow {
		c.mu.Unlock()
		return connError{ErrCodeFlowControl, "DATA exceeds the connection window"}
	}
	c.recvWindow -= flowLen
	st := c.streams[id]
	if st == nil || st.reset || st.remoteClosed {
		// Nobody will read this data, so return its flow-control credit at once.
		inc := c.creditConnLocked(flowLen)
		idle := id > c.maxClientID
		recent := c.recentlyReset(id)
		c.mu.Unlock()
		if err := c.sendWindowUpdates(0, 0, inc); err != nil {
			return err
		}
		switch {
		case idle:
			return connError{ErrCodeProtocol, "DATA on an idle stream"}
		case recent:
			return nil
		}
		return streamError{id, ErrCodeStreamClosed, "DATA on a closed stream"}
	}
	if flowLen > st.recvWindow {
		c.mu.Unlock()
		return streamError{id, ErrCodeFlowControl, "DATA exceeds the stream window"}
	}
	st.recvWindow -= flowLen
	st.received += int64(len(data))
	end := h.has(flagEndStream)
	if st.declared >= 0 && (st.received > st.declared || end && st.received != st.declared) {
		c.mu.Unlock()
		return streamError{id, ErrCodeProtocol, "DATA does not match content-length"}
	}
	// Padding and dropped data are never read by the handler, so their credit is returned now.
	credit := int64(pad)
	if len(data) > 0 && !st.body.write(data) {
		credit += int64(len(data))
	}
	var sinc uint32
	cinc := c.creditConnLocked(credit)
	if credit > 0 && !end {
		st.recvWindow += credit
		sinc = uint32(credit)
	}
	if end {
		st.remoteClosed = true
		st.body.closeWithEOF()
		c.stopReadTimerLocked(st)
	}
	c.mu.Unlock()
	if len(data) == 0 && !end && pad == 0 {
		if err := c.spendCalm(); err != nil {
			return err
		}
	}
	return c.sendWindowUpdates(id, sinc, cinc)
}

// creditConnLocked returns n bytes of connection receive window and reports how much to send in a
// WINDOW_UPDATE now; small credits are batched until half the window is due.
func (c *Conn) creditConnLocked(n int64) uint32 {
	c.recvUnacked += n
	if c.recvUnacked < int64(c.cfg.ConnWindow)/2 {
		return 0
	}
	inc := c.recvUnacked
	c.recvWindow += inc
	c.recvUnacked = 0
	return uint32(inc)
}

func (c *Conn) sendWindowUpdates(id, sinc, cinc uint32) error {
	if sinc == 0 && cinc == 0 {
		return nil
	}
	return c.writeControl(c.ctrl, func(b []byte) []byte {
		if cinc > 0 {
			b = appendWindowUpdate(b, 0, cinc)
		}
		if sinc > 0 {
			b = appendWindowUpdate(b, id, sinc)
		}
		return b
	})
}

// consumed returns flow-control credit for body bytes the handler has read.
func (c *Conn) consumed(st *stream, n int64) {
	var sinc uint32
	c.mu.Lock()
	if !st.remoteClosed && !st.reset {
		st.recvUnacked += n
		if st.recvUnacked >= int64(c.cfg.StreamWindow)/2 {
			sinc = uint32(st.recvUnacked)
			st.recvWindow += st.recvUnacked
			st.recvUnacked = 0
		}
	}
	cinc := c.creditConnLocked(n)
	c.mu.Unlock()
	if sinc != 0 || cinc != 0 {
		_ = c.writeControl(st.waiter, func(b []byte) []byte {
			if cinc > 0 {
				b = appendWindowUpdate(b, 0, cinc)
			}
			if sinc > 0 {
				b = appendWindowUpdate(b, st.id, sinc)
			}
			return b
		})
	}
}

// resetStream resets a stream after a stream error and tells the client.
func (c *Conn) resetStream(id uint32, code ErrCode) {
	c.mu.Lock()
	if st := c.streams[id]; st != nil && !st.reset {
		c.resetLocked(st)
	}
	c.rememberReset(id)
	c.mu.Unlock()
	_ = c.writeControl(c.ctrl, func(b []byte) []byte { return appendRST(b, id, code) })
}

// resetLocked marks st reset: its handler's context is canceled and its reads and writes fail.
func (c *Conn) resetLocked(st *stream) {
	st.reset = true
	c.uncountLocked(st)
	c.stopReadTimerLocked(st)
	st.body.fail(errStreamReset)
	st.x.Cancel()
	c.flow.Broadcast()
}

func (c *Conn) uncountLocked(st *stream) {
	if st.counted {
		st.counted = false
		c.open--
	}
}

func (c *Conn) rememberReset(id uint32) {
	c.resets[c.resetPos] = id
	c.resetPos = (c.resetPos + 1) % recentResets
}

func (c *Conn) recentlyReset(id uint32) bool {
	for _, r := range c.resets {
		if r == id {
			return true
		}
	}
	return false
}

func (c *Conn) stopReadTimerLocked(st *stream) {
	if st.timer != nil {
		st.timer.Stop()
		st.timer = nil
	}
}

// setIdleDeadlineLocked arms the idle timeout when no stream is active.
func (c *Conn) setIdleDeadlineLocked() {
	if len(c.streams) > 0 || c.closed {
		return
	}
	if c.goAway {
		c.Close()
		return
	}
	if d := c.cfg.IdleTimeout; d > 0 {
		_ = c.nc.SetReadDeadline(time.Now().Add(d))
		c.idleSet = true
	} else if c.idleSet || c.cfg.ReadHeaderTimeout > 0 {
		_ = c.nc.SetReadDeadline(time.Time{})
		c.idleSet = false
	}
}

func (c *Conn) clearIdleDeadlineLocked() {
	if c.idleSet {
		_ = c.nc.SetReadDeadline(time.Time{})
		c.idleSet = false
	}
}

// startStream registers a decoded request and runs or queues its handler.
func (c *Conn) startStream(st *stream) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		st.x.Release()
		return nil
	}
	c.streams[st.id] = st
	c.clearIdleDeadlineLocked()
	// Open and half-closed streams count toward the limit (RFC 9113 section 5.1.2).
	st.counted = true
	c.open++
	if !st.remoteClosed {
		if d := c.cfg.ReadTimeout; d > 0 {
			st.timer = time.AfterFunc(d, func() { c.timeoutStream(st) })
		}
	}
	if c.running < int(c.cfg.MaxConcurrentStreams) {
		c.running++
		c.wg.Add(1)
		go c.runHandlers(st)
		return nil
	}
	// Handlers of streams the client already reset may still be running; queue instead of refusing,
	// but a client that keeps resetting faster than handlers finish (Rapid Reset) is cut off.
	if len(c.pending) >= int(c.cfg.MaxConcurrentStreams) {
		return connError{ErrCodeEnhanceYourCalm, "too many streams waiting for a handler"}
	}
	c.pending = append(c.pending, st)
	return nil
}

func (c *Conn) timeoutStream(st *stream) {
	c.mu.Lock()
	live := c.streams[st.id] == st && !st.reset
	c.mu.Unlock()
	if live {
		c.resetStream(st.id, ErrCodeCancel)
	}
}

// runHandlers serves st, then any queued streams, on one goroutine.
func (c *Conn) runHandlers(st *stream) {
	defer c.wg.Done()
	for st != nil {
		c.serveStream(st)
		c.mu.Lock()
		st = nil
		if len(c.pending) > 0 {
			st = c.pending[0]
			c.pending[0] = nil
			c.pending = c.pending[1:]
		} else {
			c.running--
		}
		c.mu.Unlock()
	}
}

func (c *Conn) serveStream(st *stream) {
	var wt *time.Timer
	if d := c.cfg.WriteTimeout; d > 0 {
		wt = time.AfterFunc(d, func() { c.timeoutStream(st) })
	}
	c.mu.Lock()
	skip := st.reset
	c.mu.Unlock()
	var err error
	if !skip {
		err = st.x.Serve(&st.sink)
	}
	if wt != nil {
		wt.Stop()
	}
	c.closeStream(st, err)
}

// closeStream retires a stream whose handler has returned.
func (c *Conn) closeStream(st *stream, serveErr error) {
	if serveErr != nil {
		c.logDebug("qilat: HTTP/2 stream failed", serveErr)
	}
	var code ErrCode
	sendRST := false
	c.mu.Lock()
	switch {
	case st.reset:
	case !st.sink.ended:
		sendRST, code = true, ErrCodeInternal
	case !st.remoteClosed:
		// The response is complete but the client is still sending; tell it to stop (RFC 9113 section 8.1).
		sendRST, code = true, ErrCodeNo
	}
	if !st.reset {
		c.resetLocked(st)
	}
	if sendRST {
		c.rememberReset(st.id)
	}
	delete(c.streams, st.id)
	// Body bytes the handler never read still hold connection window.
	cinc := c.creditConnLocked(st.body.drop())
	c.setIdleDeadlineLocked()
	c.mu.Unlock()
	if sendRST || cinc > 0 {
		_ = c.writeControl(st.waiter, func(b []byte) []byte {
			if sendRST {
				b = appendRST(b, st.id, code)
			}
			if cinc > 0 {
				b = appendWindowUpdate(b, 0, cinc)
			}
			return b
		})
	}
	st.x.Release()
}

// finish tears the connection down after the reader stops.
func (c *Conn) finish() {
	c.mu.Lock()
	c.closed = true
	for _, st := range c.streams {
		if !st.reset {
			c.resetLocked(st)
		}
	}
	pending := c.pending
	c.pending = nil
	for _, st := range pending {
		delete(c.streams, st.id)
	}
	c.flow.Broadcast()
	c.mu.Unlock()
	if c.hb.x != nil {
		c.hb.x.Release()
		c.hb.x = nil
	}
	for _, st := range pending {
		st.body.drop()
		st.x.Release()
	}
	c.Close()
	c.wg.Wait()
	c.sched.lock(c.ctrl)
	if c.bw != nil {
		putWriter(c.bw)
		c.bw = nil
	}
	if c.writeErr == nil {
		c.writeErr = errConnClosed
	}
	c.sched.unlock()
	if c.ownsBR {
		putReader(c.br)
	}
	c.br = nil
}

// writeControl writes the frames built by build and flushes.
func (c *Conn) writeControl(w *waiter, build func([]byte) []byte) error {
	c.sched.lock(w)
	if c.writeErr != nil {
		err := c.writeErr
		c.sched.unlock()
		return err
	}
	c.wbuf = build(c.wbuf[:0])
	c.write(c.wbuf)
	return c.unlockSched(true)
}

// write buffers p; failures are recorded in writeErr. Callers hold sched.
func (c *Conn) write(p []byte) {
	if c.writeErr != nil {
		return
	}
	if c.bw == nil {
		c.bw = getWriter(c.nc)
	}
	if _, err := c.bw.Write(p); err != nil {
		c.failWrite(err)
	}
}

func (c *Conn) failWrite(err error) {
	c.writeErr = err
	c.Close()
}

// unlockSched releases the write lock. A requested flush is done by the last writer in line, so
// concurrent responses share one syscall.
func (c *Conn) unlockSched(flush bool) error {
	if flush {
		c.needFlush = true
	}
	if c.needFlush && c.writeErr == nil && !c.sched.hasWaiters() {
		c.needFlush = false
		if c.bw != nil {
			if err := c.bw.Flush(); err != nil {
				c.failWrite(err)
			}
		}
	}
	err := c.writeErr
	c.sched.unlock()
	return err
}

func (c *Conn) flush(w *waiter) error {
	c.sched.lock(w)
	return c.unlockSched(true)
}

// writeHeaders sends a HEADERS frame (and CONTINUATION frames if needed). status 0 sends trailers.
func (c *Conn) writeHeaders(st *stream, status int, h exchange.HeaderList, endStream, flush bool) error {
	c.mu.Lock()
	reset := st.reset
	c.mu.Unlock()
	if reset {
		return errStreamReset
	}
	c.sched.lock(st.waiter)
	if c.writeErr != nil {
		err := c.writeErr
		c.sched.unlock()
		return err
	}
	block := c.enc.startBlock(c.wbuf[:0])
	if status != 0 {
		block = c.enc.appendStatus(block, status)
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
			block = c.enc.appendField(block, name, value)
		}
	}
	if status >= 200 && !hasDate {
		block = c.enc.appendField(block, httpfields.DateName, httpfields.Date())
	}
	c.wbuf = block
	c.writeHeaderBlock(st.id, block, endStream)
	return c.unlockSched(flush || endStream)
}

// writeHeaderBlock splits an encoded block into HEADERS and CONTINUATION frames. Callers hold sched.
func (c *Conn) writeHeaderBlock(id uint32, block []byte, endStream bool) {
	maxFrame := int(c.peerFrame.Load())
	typ, flags := frameHeaders, uint8(0)
	if endStream {
		flags = flagEndStream
	}
	for first := true; first || len(block) > 0; first = false {
		n := min(len(block), maxFrame)
		f := flags
		if n == len(block) {
			f |= flagEndHeaders
		}
		c.write(appendFrameHeader(c.fhdr[:0], n, typ, f, id))
		c.write(block[:n])
		block = block[n:]
		typ, flags = frameContinuation, 0
	}
}

// writeData sends p as DATA frames within the flow-control windows, ending the stream if end is set.
func (c *Conn) writeData(st *stream, p []byte, end bool) error {
	if len(p) == 0 && !end {
		return nil
	}
	for {
		n := 0
		if len(p) > 0 {
			var err error
			if n, err = c.reserve(st, len(p)); err != nil {
				return err
			}
		} else {
			c.mu.Lock()
			reset := st.reset
			c.mu.Unlock()
			if reset {
				return errStreamReset
			}
		}
		last := n == len(p)
		var flags uint8
		if last && end {
			flags = flagEndStream
		}
		c.sched.lock(st.waiter)
		if c.writeErr != nil {
			err := c.writeErr
			c.sched.unlock()
			return err
		}
		c.write(appendFrameHeader(c.fhdr[:0], n, frameData, flags, st.id))
		c.write(p[:n])
		p = p[n:]
		if err := c.unlockSched(last && end); err != nil {
			return err
		}
		if last {
			return nil
		}
	}
}

// reserve takes up to want bytes of send window for st, waiting for WINDOW_UPDATE if there is none.
func (c *Conn) reserve(st *stream, want int) (int, error) {
	maxFrame := int64(c.peerFrame.Load())
	c.mu.Lock()
	defer c.mu.Unlock()
	flushed := false
	for {
		switch {
		case c.closed:
			return 0, errConnClosed
		case st.reset:
			return 0, errStreamReset
		}
		if n := min(int64(want), maxFrame, st.sendWindow, c.sendWindow); n > 0 {
			st.sendWindow -= n
			c.sendWindow -= n
			return int(n), nil
		}
		if !flushed {
			// The client sends WINDOW_UPDATE only after it sees our data, so flush before waiting.
			c.mu.Unlock()
			_ = c.flush(st.waiter)
			c.mu.Lock()
			flushed = true
			continue
		}
		c.flow.Wait()
	}
}

// headerBlock collects one HEADERS block as it is decoded. HPACK state must stay in sync with the client,
// so a block is always decoded fully even when the request will be rejected.
type headerBlock struct {
	active    bool
	id        uint32
	endStream bool
	trailer   bool
	discard   bool    // decode only to keep the HPACK state; no request is created
	resetCode ErrCode // reset the stream with this code after decoding, when resetting
	resetting bool
	frames    int
	x         exchange.Exchange
	req       httpfields.Request
}

func (hb *headerBlock) reset(id uint32) {
	req := hb.req
	*hb = headerBlock{id: id, active: true, req: req}
}

func (c *Conn) onHeaders(h frameHeader, p []byte) error {
	id := h.streamID
	if id == 0 || id%2 == 0 {
		return connError{ErrCodeProtocol, "HEADERS on an invalid stream ID"}
	}
	frag, _, err := stripPadding(h, p)
	if err != nil {
		return err
	}
	hb := &c.hb
	hb.reset(id)
	hb.endStream = h.has(flagEndStream)
	if h.has(flagPriority) {
		if len(frag) < 5 {
			return connError{ErrCodeFrameSize, "HEADERS too short for priority"}
		}
		if be32(frag)&maxStreamID == id {
			hb.resetting, hb.resetCode = true, ErrCodeProtocol
		}
		frag = frag[5:]
	}

	c.mu.Lock()
	st := c.streams[id]
	switch {
	case st != nil:
		c.mu.Unlock()
		hb.trailer = true
		switch {
		case st.remoteClosed || st.reset:
			hb.discard, hb.resetting, hb.resetCode = true, true, ErrCodeStreamClosed
		}
	case id <= c.maxClientID:
		recent := c.recentlyReset(id)
		c.mu.Unlock()
		if !recent && id < c.maxClientID {
			return connError{ErrCodeProtocol, "HEADERS on a stream ID lower than a previous one"}
		}
		return connError{ErrCodeStreamClosed, "HEADERS on a closed stream"}
	default:
		c.maxClientID = id
		ignore := c.goAway && c.goAwayID < id || c.closed
		refuse := c.open >= c.cfg.MaxConcurrentStreams
		c.mu.Unlock()
		switch {
		case ignore:
			hb.discard = true
		case refuse:
			hb.discard, hb.resetting, hb.resetCode = true, true, ErrCodeRefusedStream
		default:
			hb.x = c.cfg.Acquire()
		}
	}
	hb.req.Reset(hb.x, hb.trailer, int(c.cfg.MaxHeaderListSize))
	return c.onHeaderFragment(h, frag)
}

func (c *Conn) onHeaderFragment(h frameHeader, frag []byte) error {
	hb := &c.hb
	if hb.frames++; hb.frames > maxBlockFrames {
		return connError{ErrCodeEnhanceYourCalm, "too many CONTINUATION frames"}
	}
	if len(frag) == 0 && !h.has(flagEndHeaders) {
		if err := c.spendCalm(); err != nil {
			return err
		}
	}
	if _, err := c.dec.Write(frag); err != nil {
		if errors.Is(err, hpack.ErrStringLength) {
			return connError{ErrCodeEnhanceYourCalm, "header field too long"}
		}
		return connError{ErrCodeCompression, err.Error()}
	}
	if !h.has(flagEndHeaders) {
		return nil
	}
	hb.active = false
	if err := c.dec.Close(); err != nil {
		return connError{ErrCodeCompression, err.Error()}
	}
	return c.endHeaderBlock()
}

func (c *Conn) onField(f hpack.HeaderField) {
	if !c.hb.discard {
		c.hb.req.Field(bytesconv.S2B(f.Name), bytesconv.S2B(f.Value))
	}
}

func (c *Conn) endHeaderBlock() error {
	hb := &c.hb
	req := &hb.req
	x := hb.x
	hb.x = nil
	release := func() {
		if x != nil {
			x.Release()
		}
	}
	switch {
	case hb.resetting:
		release()
		return streamError{hb.id, hb.resetCode, "stream reset"}
	case hb.discard:
		release()
		return nil
	case hb.trailer:
		if req.Malformed != "" || req.TooLarge || !hb.endStream {
			return streamError{hb.id, ErrCodeProtocol, "malformed trailers"}
		}
		return c.onTrailers(hb.id)
	case req.TooLarge:
		release()
		return c.rejectStream(hb.id, 431, hb.endStream)
	}
	if msg := req.Finish(2); msg != "" {
		release()
		return streamError{hb.id, ErrCodeProtocol, msg}
	}
	if hb.endStream && req.ContentLength > 0 {
		release()
		return streamError{hb.id, ErrCodeProtocol, "content-length without DATA"}
	}
	if limit := c.cfg.MaxRequestBodySize; limit >= 0 && req.ContentLength > limit {
		release()
		return c.rejectStream(hb.id, 413, hb.endStream)
	}
	x.SetConn(c.remote, c.tlsState)

	st := newStream(c, hb.id, x)
	st.remoteClosed = hb.endStream
	st.declared = req.ContentLength
	st.expectContinue = req.ExpectContinue && !hb.endStream
	st.setPriority(req.Urgency, req.Incremental)
	st.body.limit = c.cfg.MaxRequestBodySize
	c.mu.Lock()
	st.sendWindow = c.peerWindow
	c.mu.Unlock()
	st.recvWindow = int64(c.cfg.StreamWindow)
	if hb.endStream {
		x.SetBody(nil, 0)
	} else {
		x.SetBody(&st.body, req.ContentLength)
	}
	return c.startStream(st)
}

func (c *Conn) onTrailers(id uint32) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	st := c.streams[id]
	if st == nil || st.reset {
		return nil
	}
	if st.declared >= 0 && st.received != st.declared {
		return streamError{id, ErrCodeProtocol, "DATA does not match content-length"}
	}
	st.remoteClosed = true
	c.stopReadTimerLocked(st)
	st.body.closeWithEOF()
	return nil
}

// rejectStream answers a request with an error status without running a handler.
func (c *Conn) rejectStream(id uint32, status int, endStream bool) error {
	text := "413 Request Entity Too Large"
	if status == 431 {
		text = "431 Request Header Fields Too Large"
	}
	c.sched.lock(c.ctrl)
	if c.writeErr != nil {
		err := c.writeErr
		c.sched.unlock()
		return err
	}
	var lenBuf [4]byte
	block := c.enc.appendStatus(c.enc.startBlock(c.wbuf[:0]), status)
	block = c.enc.appendField(block, []byte("content-type"), []byte("text/plain; charset=utf-8"))
	block = c.enc.appendField(block, []byte("content-length"), appendUint(lenBuf[:0], len(text)))
	c.wbuf = block
	c.writeHeaderBlock(id, block, false)
	c.write(appendFrameHeader(c.fhdr[:0], len(text), frameData, flagEndStream, id))
	c.write([]byte(text))
	if !endStream {
		c.write(appendRST(c.fhdr[:0], id, ErrCodeNo))
	}
	c.mu.Lock()
	c.rememberReset(id)
	c.mu.Unlock()
	return c.unlockSched(true)
}

func appendUint(b []byte, n int) []byte {
	if n >= 10 {
		b = appendUint(b, n/10)
	}
	return append(b, byte('0'+n%10))
}

func getReader(r io.Reader) *bufio.Reader {
	if v := readerPool.Get(); v != nil {
		br := v.(*bufio.Reader)
		br.Reset(r)
		return br
	}
	return bufio.NewReaderSize(r, writeBufferSize)
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
	return bufio.NewWriterSize(w, writeBufferSize)
}

func putWriter(bw *bufio.Writer) {
	bw.Reset(nil)
	writerPool.Put(bw)
}
