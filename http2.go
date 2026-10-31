package qilat

import (
	"io"
	"net"
	"time"

	"github.com/erizeta/qilat/internal/exchange"
	"github.com/erizeta/qilat/internal/h2"
)

// Default HTTP/2 limits used when the corresponding HTTP2Config field is zero.
const (
	DefaultHTTP2MaxConcurrentStreams          = 250
	DefaultHTTP2MaxReadFrameSize              = 16 << 10
	DefaultHTTP2MaxReceiveBufferPerConnection = 1 << 20
	DefaultHTTP2MaxReceiveBufferPerStream     = 1 << 20
	DefaultHTTP2MaxDecoderHeaderTableSize     = 4 << 10
)

// HTTP2Config configures HTTP/2. Zero fields mean the defaults above; values outside the range HTTP/2 allows
// are clamped. Server.MaxHeaderBytes limits the decoded header list, and Server.MaxRequestBodySize the body.
//
// Protections against known HTTP/2 attacks are always on: streams reset faster than their handlers finish
// (Rapid Reset, CVE-2023-44487), CONTINUATION floods, and floods of PING, SETTINGS, RST_STREAM, PRIORITY,
// or empty frames end the connection with GOAWAY ENHANCE_YOUR_CALM.
type HTTP2Config struct {
	// MaxConcurrentStreams limits open streams, and running handlers, per connection.
	MaxConcurrentStreams int
	// MaxReadFrameSize is the largest frame the server accepts (16 KiB to 16 MiB).
	MaxReadFrameSize int
	// MaxReceiveBufferPerConnection and MaxReceiveBufferPerStream bound request body data buffered before
	// handlers read it, which is how much the client may send ahead (at least 64 KiB).
	MaxReceiveBufferPerConnection int
	MaxReceiveBufferPerStream     int
	// MaxDecoderHeaderTableSize is the HPACK dynamic table size the client may use for request headers.
	MaxDecoderHeaderTableSize int
}

func (s *Server) h2Config(xcfg *exchangeConfig, maxHeader int, maxBody int64) *h2.Config {
	c := s.HTTP2
	clamp := func(v, def, lo, hi int) uint32 {
		if v <= 0 {
			v = def
		}
		return uint32(min(max(v, lo), hi)) //nolint:gosec // clamped to hi, at most 1<<32-1
	}
	const maxWindow = 1<<31 - 1
	return &h2.Config{
		Acquire:              func() exchange.Exchange { return acquireExchange(xcfg) },
		MaxConcurrentStreams: clamp(c.MaxConcurrentStreams, DefaultHTTP2MaxConcurrentStreams, 1, 1<<31-1),
		MaxReadFrameSize:     clamp(c.MaxReadFrameSize, DefaultHTTP2MaxReadFrameSize, 1<<14, 1<<24-1),
		MaxHeaderListSize:    uint32(min(maxHeader, 1<<32-1)), //nolint:gosec // clamped to the uint32 range
		HeaderTableSize:      clamp(c.MaxDecoderHeaderTableSize, DefaultHTTP2MaxDecoderHeaderTableSize, 0, 1<<32-1),
		StreamWindow:         clamp(c.MaxReceiveBufferPerStream, DefaultHTTP2MaxReceiveBufferPerStream, 65535, maxWindow),
		ConnWindow:           clamp(c.MaxReceiveBufferPerConnection, DefaultHTTP2MaxReceiveBufferPerConnection, 65535, maxWindow),
		MaxRequestBodySize:   maxBody,
		ReadHeaderTimeout:    timeoutOr(s.ReadHeaderTimeout, DefaultReadHeaderTimeout),
		ReadTimeout:          timeoutOr(s.ReadTimeout, 0),
		WriteTimeout:         timeoutOr(s.WriteTimeout, 0),
		ReadStallTimeout:     timeoutOr(s.ReadStallTimeout, DefaultStallTimeout),
		WriteStallTimeout:    timeoutOr(s.WriteStallTimeout, DefaultStallTimeout),
		IdleTimeout:          timeoutOr(s.IdleTimeout, DefaultIdleTimeout),
		Logger:               xcfg.logger,
	}
}

// sniffH2C reads until the bytes received either diverge from the HTTP/2 preface or complete its first line,
// "PRI * HTTP/2.0", which no HTTP/1.1 request uses. A client that sends that line is handed to HTTP/2 even if
// the rest of its preface is wrong, so it gets an HTTP/2 error. The bytes read are returned to be replayed.
func sniffH2C(nc net.Conn, timeout time.Duration) (prefix []byte, isH2 bool, err error) {
	if timeout > 0 {
		_ = nc.SetReadDeadline(time.Now().Add(timeout))
		defer func() { _ = nc.SetReadDeadline(time.Time{}) }()
	}
	const firstLine = len("PRI * HTTP/2.0\r\n")
	buf := make([]byte, firstLine)
	n := 0
	for n < len(buf) {
		m, rerr := nc.Read(buf[n:])
		n += m
		if string(buf[:n]) != h2.ClientPreface[:n] {
			return buf[:n], false, nil
		}
		if rerr != nil {
			return buf[:n], false, rerr
		}
	}
	return buf, true, nil
}

// prefixConn replays bytes already read from the connection before reading from it again.
type prefixConn struct {
	net.Conn
	prefix []byte
}

func (c *prefixConn) Read(p []byte) (int, error) {
	if len(c.prefix) > 0 {
		n := copy(p, c.prefix)
		c.prefix = c.prefix[n:]
		return n, nil
	}
	return c.Conn.Read(p)
}

// ReadFrom keeps sendfile available on HTTP/1.1 connections served with H2C enabled.
func (c *prefixConn) ReadFrom(r io.Reader) (int64, error) {
	if rf, ok := c.Conn.(io.ReaderFrom); ok {
		return rf.ReadFrom(r)
	}
	return copyBuffered(c.Conn, r)
}
