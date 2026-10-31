package qilat

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/erizeta/qilat/internal/exchange"
	"github.com/erizeta/qilat/internal/h3"
)

// Default HTTP/3 settings used when the corresponding HTTP3Config field is zero.
const (
	DefaultHTTP3MaxConcurrentStreams          = 250
	DefaultHTTP3MaxReceiveBufferPerConnection = 4 << 20
	DefaultHTTP3MaxReceiveBufferPerStream     = 1 << 20
	DefaultAltSvcMaxAge                       = 24 * time.Hour
)

// HTTP3Config configures HTTP/3. Zero fields mean the defaults above. Server.MaxHeaderBytes limits the field
// section, Server.MaxRequestBodySize the body, Server.IdleTimeout the QUIC idle timeout, and
// Server.ReadHeaderTimeout the QUIC handshake.
type HTTP3Config struct {
	// MaxConcurrentStreams limits concurrent request streams, and running handlers, per connection.
	MaxConcurrentStreams int
	// MaxReceiveBufferPerConnection and MaxReceiveBufferPerStream bound the QUIC flow-control windows, which is
	// how much request data a client may send ahead of the handlers reading it.
	MaxReceiveBufferPerConnection int
	MaxReceiveBufferPerStream     int
	// Allow0RTT accepts requests in 0-RTT data, saving a round trip for returning clients. 0-RTT data can be
	// replayed, so requests other than GET, HEAD, and OPTIONS are answered 425 Too Early (RFC 8470).
	Allow0RTT bool
	// AltSvcMaxAge is how long clients may remember that HTTP/3 is available (the Alt-Svc "ma" parameter).
	AltSvcMaxAge time.Duration
	// DisableAltSvc stops adding the Alt-Svc header to HTTP/1.1 and HTTP/2 responses.
	DisableAltSvc bool
}

// ServeQUIC serves HTTP/3 on the UDP connection pc, using TLSConfig, which must hold certificates. It always
// closes pc, after the connections it accepted have finished, and returns ErrServerClosed after Shutdown or
// Close. While it runs, HTTP/1.1 and HTTP/2 responses from this Server advertise it with Alt-Svc.
func (s *Server) ServeQUIC(pc net.PacketConn) error {
	if s.TLSConfig == nil || len(s.TLSConfig.Certificates) == 0 && s.TLSConfig.GetCertificate == nil {
		_ = pc.Close()
		return errors.New("qilat: ServeQUIC needs TLSConfig with certificates")
	}
	return s.serveQUIC(pc, s.TLSConfig)
}

func (s *Server) serveQUIC(pc net.PacketConn, tlsConf *tls.Config) error {
	defer func() { _ = pc.Close() }()
	if s.Handler == nil {
		return errors.New("qilat: Server.Handler is nil")
	}
	xcfg := s.exchangeConfig(nil)
	maxHeader, maxBody := s.limits()
	c := s.HTTP3
	ln, err := h3.Listen(pc, tlsConf, h3.QUICConfig{
		MaxIncomingStreams:         int64(positiveOr(c.MaxConcurrentStreams, DefaultHTTP3MaxConcurrentStreams)),
		MaxIdleTimeout:             timeoutOr(s.IdleTimeout, DefaultIdleTimeout),
		HandshakeIdleTimeout:       timeoutOr(s.ReadHeaderTimeout, DefaultReadHeaderTimeout),
		MaxStreamReceiveWindow:     uint64(positiveOr(c.MaxReceiveBufferPerStream, DefaultHTTP3MaxReceiveBufferPerStream)),         //nolint:gosec // positive
		MaxConnectionReceiveWindow: uint64(positiveOr(c.MaxReceiveBufferPerConnection, DefaultHTTP3MaxReceiveBufferPerConnection)), //nolint:gosec // positive
		Allow0RTT:                  c.Allow0RTT,
	}, &h3.Config{
		Acquire:              func() exchange.Exchange { return acquireExchange(xcfg) },
		MaxFieldSectionSize:  min(maxHeader, 1<<30),
		MaxRequestBodySize:   maxBody,
		MaxConcurrentStreams: positiveOr(c.MaxConcurrentStreams, DefaultHTTP3MaxConcurrentStreams),
		ReadHeaderTimeout:    timeoutOr(s.ReadHeaderTimeout, DefaultReadHeaderTimeout),
		ReadTimeout:          timeoutOr(s.ReadTimeout, 0),
		WriteTimeout:         timeoutOr(s.WriteTimeout, 0),
		ReadStallTimeout:     timeoutOr(s.ReadStallTimeout, DefaultStallTimeout),
		WriteStallTimeout:    timeoutOr(s.WriteStallTimeout, DefaultStallTimeout),
		Allow0RTT:            c.Allow0RTT,
		Logger:               xcfg.logger,
	})
	if err != nil {
		return err
	}
	if !s.trackListener(ln) {
		_ = ln.Close()
		return ErrServerClosed
	}
	defer s.untrackListener(ln)
	s.advertiseHTTP3(ln.Addr())
	defer s.altSvc.Store(nil)

	// The QUIC connections share pc, so it stays open until they have all finished.
	var conns sync.WaitGroup
	defer conns.Wait()
	for {
		conn, err := ln.Accept(context.Background())
		if err != nil {
			if s.shuttingDown.Load() {
				return ErrServerClosed
			}
			return err
		}
		if !s.trackConn(conn) {
			conn.Close()
			continue
		}
		conns.Add(1)
		go func() {
			defer conns.Done()
			defer s.untrackConn(conn)
			conn.Serve()
		}()
	}
}

func (s *Server) advertiseHTTP3(addr net.Addr) {
	if s.HTTP3.DisableAltSvc {
		return
	}
	udp, ok := addr.(*net.UDPAddr)
	if !ok {
		return
	}
	maxAge := s.HTTP3.AltSvcMaxAge
	if maxAge <= 0 {
		maxAge = DefaultAltSvcMaxAge
	}
	v := []byte(`h3=":` + strconv.Itoa(udp.Port) + `"; ma=` + strconv.FormatInt(int64(maxAge/time.Second), 10))
	s.altSvc.Store(&v)
}

func positiveOr(v, def int) int {
	if v <= 0 {
		return def
	}
	return v
}
