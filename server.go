package qilat

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"log/slog"
	"math"
	"net"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/erizeta/qilat/internal/exchange"
	"github.com/erizeta/qilat/internal/h1"
	"github.com/erizeta/qilat/internal/h2"
)

// Default limits used when the corresponding Server field is zero.
const (
	DefaultReadHeaderTimeout  = 10 * time.Second
	DefaultIdleTimeout        = 120 * time.Second
	DefaultMaxHeaderBytes     = 64 << 10
	DefaultMaxRequestBodySize = 4 << 20
)

// ErrServerClosed is returned by Serve and ListenAndServe after Shutdown or Close.
var ErrServerClosed = errors.New("qilat: Server closed")

// Server is an HTTP server. Unlike net/http, zero limits mean safe defaults rather than no limit;
// set a negative value to disable a limit. A Server must not be copied after first use.
type Server struct {
	// Handler serves every request. It must not be nil.
	Handler Handler
	// TLSConfig is used by ServeTLS and ListenAndServeTLS. It is cloned, never modified.
	TLSConfig *tls.Config

	// ReadHeaderTimeout bounds reading a request line and header. Zero means DefaultReadHeaderTimeout.
	ReadHeaderTimeout time.Duration
	// ReadTimeout bounds reading a whole request, body included. Zero means no limit, so slow uploads work.
	ReadTimeout time.Duration
	// WriteTimeout bounds writing a response, measured from the end of the request header. Zero means no limit.
	WriteTimeout time.Duration
	// IdleTimeout bounds the wait for the next keep-alive request. Zero means DefaultIdleTimeout.
	IdleTimeout time.Duration
	// MaxHeaderBytes limits the request line and header. Zero means DefaultMaxHeaderBytes.
	MaxHeaderBytes int
	// MaxRequestBodySize limits the request body; larger bodies get 413. Zero means DefaultMaxRequestBodySize.
	MaxRequestBodySize int64
	// MaxConcurrentConns limits open connections; Accept waits while the limit is reached. Zero means no limit.
	MaxConcurrentConns int

	// HTTP2 configures HTTP/2, which TLS connections negotiate through ALPN unless DisableHTTP2 is set.
	HTTP2 HTTP2Config
	// H2C serves cleartext HTTP/2 to clients that start with the HTTP/2 preface (prior knowledge), on the
	// same listener as HTTP/1.1. The deprecated HTTP/1.1 Upgrade to h2c is not supported.
	H2C bool
	// DisableHTTP2 serves HTTP/1.1 only, even if TLSConfig.NextProtos lists "h2".
	DisableHTTP2 bool
	// HTTP3 configures HTTP/3, which ListenAndServeTLS and ServeQUIC serve over UDP.
	HTTP3 HTTP3Config
	// DisableHTTP3 makes ListenAndServeTLS serve TCP only.
	DisableHTTP3 bool

	// ReduceMemoryUsage returns read buffers to a pool while connections are idle, costing one extra read per request.
	ReduceMemoryUsage bool
	// DebugPoison makes handlers that use a request or ResponseWriter after ServeHTTP returns panic. It disables pooling.
	DebugPoison bool

	// BaseContext returns the base context for requests from ln, which is nil for HTTP/3. If BaseContext is nil,
	// context.Background is used.
	BaseContext func(ln net.Listener) context.Context
	// ErrorLog receives handler panics and connection errors. If nil, slog.Default is used.
	ErrorLog *slog.Logger

	mu           sync.Mutex
	listeners    map[io.Closer]struct{}
	altSvc       atomic.Pointer[[]byte] // Alt-Svc value advertising HTTP/3, while a QUIC listener runs
	conns        map[serverConn]struct{}
	cancels      []context.CancelFunc
	shuttingDown atomic.Bool
}

// ListenAndServe listens on the TCP address addr and serves HTTP/1.1, and cleartext HTTP/2 if H2C is set.
// An empty addr means ":http".
func (s *Server) ListenAndServe(addr string) error {
	if s.shuttingDown.Load() {
		return ErrServerClosed
	}
	if addr == "" {
		addr = ":http"
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	return s.Serve(ln)
}

// ListenAndServeTLS listens on addr and serves HTTPS: HTTP/2 and HTTP/1.1 over TCP, chosen through ALPN, and
// HTTP/3 over UDP on the same port unless DisableHTTP3 is set. HTTP/1.1 and HTTP/2 responses then carry an
// Alt-Svc header so clients can switch to HTTP/3. If either listener fails, the other is closed and the error
// is returned. An empty addr means ":https". The certificate files may be empty if TLSConfig already holds
// certificates.
func (s *Server) ListenAndServeTLS(addr, certFile, keyFile string) error {
	if s.shuttingDown.Load() {
		return ErrServerClosed
	}
	if addr == "" {
		addr = ":https"
	}
	config, err := s.tlsConfig(certFile, keyFile)
	if err != nil {
		return err
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	if s.DisableHTTP3 {
		return s.Serve(tls.NewListener(ln, config))
	}
	// A port of 0 picks a free TCP port; HTTP/3 must use the same number for Alt-Svc to point at it.
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		_ = ln.Close()
		return err
	}
	_, port, _ := net.SplitHostPort(ln.Addr().String())
	pc, err := net.ListenPacket("udp", net.JoinHostPort(host, port))
	if err != nil {
		_ = ln.Close()
		return err
	}
	errc := make(chan error, 2)
	go func() { errc <- s.Serve(tls.NewListener(ln, config)) }()
	go func() { errc <- s.serveQUIC(pc, config) }()
	first := <-errc
	if !errors.Is(first, ErrServerClosed) {
		// One protocol failed; stop the other so the caller sees a single, complete failure.
		_ = ln.Close()
		_ = pc.Close()
	}
	second := <-errc
	if errors.Is(first, ErrServerClosed) && !errors.Is(second, ErrServerClosed) {
		return second
	}
	return first
}

// ServeTLS serves HTTPS on ln. See [Server.ListenAndServeTLS] for the certificate arguments. It serves
// HTTP/1.1 and HTTP/2 only; use [Server.ServeQUIC] to add HTTP/3.
func (s *Server) ServeTLS(ln net.Listener, certFile, keyFile string) error {
	config, err := s.tlsConfig(certFile, keyFile)
	if err != nil {
		_ = ln.Close()
		return err
	}
	return s.Serve(tls.NewListener(ln, config))
}

// tlsConfig clones TLSConfig, offers h2 through ALPN, and loads the certificate files if given.
func (s *Server) tlsConfig(certFile, keyFile string) (*tls.Config, error) {
	var config *tls.Config
	if s.TLSConfig != nil {
		config = s.TLSConfig.Clone()
	} else {
		config = &tls.Config{MinVersion: tls.VersionTLS12}
	}
	if len(config.NextProtos) == 0 {
		config.NextProtos = []string{"http/1.1"}
	}
	if !s.DisableHTTP2 && !slices.Contains(config.NextProtos, "h2") {
		config.NextProtos = append([]string{"h2"}, config.NextProtos...)
	}
	if certFile != "" || keyFile != "" || (len(config.Certificates) == 0 && config.GetCertificate == nil) {
		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return nil, err
		}
		config.Certificates = append(config.Certificates, cert)
	}
	return config, nil
}

// Serve accepts connections on ln and serves each in its own goroutine. It always closes ln and
// returns ErrServerClosed after Shutdown or Close.
func (s *Server) Serve(ln net.Listener) error {
	if s.Handler == nil {
		_ = ln.Close()
		return errors.New("qilat: Server.Handler is nil")
	}
	if !s.trackListener(ln) {
		_ = ln.Close()
		return ErrServerClosed
	}
	defer s.untrackListener(ln)

	xcfg := s.exchangeConfig(ln)
	maxHeader, maxBody := s.limits()
	cfg := s.h1Config(xcfg, maxHeader, maxBody)
	var h2cfg *h2.Config
	if !s.DisableHTTP2 {
		h2cfg = s.h2Config(xcfg, maxHeader, maxBody)
	}

	var sem chan struct{}
	if s.MaxConcurrentConns > 0 {
		sem = make(chan struct{}, s.MaxConcurrentConns)
	}
	var backoff time.Duration
	for {
		if sem != nil {
			sem <- struct{}{}
		}
		nc, err := ln.Accept()
		if err != nil {
			if sem != nil {
				<-sem
			}
			if s.shuttingDown.Load() {
				return ErrServerClosed
			}
			if errors.Is(err, net.ErrClosed) {
				return err
			}
			// Errors such as EMFILE are usually transient; back off instead of spinning, as net/http does.
			backoff = min(max(2*backoff, 5*time.Millisecond), time.Second)
			s.logger().Error("qilat: accept error; retrying", "error", err, "delay", backoff)
			time.Sleep(backoff)
			continue
		}
		backoff = 0
		pending := handshakingConn{nc}
		if !s.trackConn(pending) {
			_ = nc.Close()
			if sem != nil {
				<-sem
			}
			continue
		}
		go func() {
			defer func() {
				if sem != nil {
					<-sem
				}
			}()
			s.serveConn(pending, cfg, h2cfg)
		}()
	}
}

// serveConn picks the protocol for a new connection and serves it. While the protocol is unknown
// (TLS handshake, h2c sniffing) the connection is tracked as busy, as an HTTP/1.1 connection reading
// its first request would be.
func (s *Server) serveConn(raw handshakingConn, cfg *h1.Config, h2cfg *h2.Config) {
	nc := raw.nc
	var c serverConn
	defer func() {
		if c == nil {
			s.untrackConn(raw)
			_ = nc.Close()
		} else {
			s.untrackConn(c)
		}
	}()
	switch tc, isTLS := nc.(*tls.Conn); {
	case isTLS && h2cfg != nil:
		if d := cfg.ReadHeaderTimeout; d > 0 {
			_ = tc.SetDeadline(time.Now().Add(d))
		}
		if err := tc.Handshake(); err != nil {
			s.logger().Debug("qilat: TLS handshake failed", "remote", nc.RemoteAddr(), "error", err)
			return
		}
		_ = tc.SetDeadline(time.Time{})
		if st := tc.ConnectionState(); st.NegotiatedProtocol == "h2" {
			hc := h2.NewConn(nc, nil, &st, h2cfg)
			c = hc
			s.serveProto(raw, hc, hc.Serve)
			return
		}
	case !isTLS && h2cfg != nil && s.H2C:
		prefix, isH2, err := sniffH2C(nc, cfg.ReadHeaderTimeout)
		if err != nil && len(prefix) == 0 {
			return
		}
		nc = &prefixConn{Conn: nc, prefix: prefix}
		if isH2 {
			hc := h2.NewConn(nc, nil, nil, h2cfg)
			c = hc
			s.serveProto(raw, hc, hc.Serve)
			return
		}
	}
	hc := h1.NewConn(nc, cfg)
	c = hc
	s.serveProto(raw, hc, hc.Serve)
}

func (s *Server) serveProto(raw handshakingConn, c serverConn, serve func()) {
	if s.replaceConn(raw, c) {
		serve()
	} else {
		c.Close()
	}
}

// Shutdown stops accepting connections, closes idle ones, and waits for active requests to finish.
// If ctx ends first, the contexts of running requests are canceled and ctx's error is returned.
func (s *Server) Shutdown(ctx context.Context) error {
	s.shuttingDown.Store(true)
	err := s.closeListeners()
	wait := time.Millisecond
	timer := time.NewTimer(wait)
	defer timer.Stop()
	for {
		if s.closeIdleConns() {
			s.cancelContexts()
			return err
		}
		select {
		case <-ctx.Done():
			s.cancelContexts()
			return ctx.Err()
		case <-timer.C:
			wait = min(2*wait, 500*time.Millisecond)
			timer.Reset(wait)
		}
	}
}

// Close immediately closes all listeners and connections, except hijacked ones, and cancels running requests.
func (s *Server) Close() error {
	s.shuttingDown.Store(true)
	err := s.closeListeners()
	s.mu.Lock()
	for c := range s.conns {
		c.Close()
	}
	s.mu.Unlock()
	s.cancelContexts()
	return err
}

// exchangeConfig builds the per-listener core configuration. ln is nil for QUIC listeners.
func (s *Server) exchangeConfig(ln net.Listener) *exchangeConfig {
	base := context.Background()
	if s.BaseContext != nil {
		if base = s.BaseContext(ln); base == nil {
			panic("qilat: BaseContext returned a nil context")
		}
	}
	ctx, cancel := context.WithCancel(base)
	s.mu.Lock()
	s.cancels = append(s.cancels, cancel)
	s.mu.Unlock()
	return &exchangeConfig{handler: s.Handler, baseCtx: ctx, poison: s.DebugPoison, logger: s.logger(), altSvc: &s.altSvc}
}

// limits resolves the header and body limits shared by every protocol.
func (s *Server) limits() (maxHeader int, maxBody int64) {
	maxHeader = s.MaxHeaderBytes
	switch {
	case maxHeader == 0:
		maxHeader = DefaultMaxHeaderBytes
	case maxHeader < 0:
		maxHeader = math.MaxInt
	}
	maxBody = s.MaxRequestBodySize
	switch {
	case maxBody == 0:
		maxBody = DefaultMaxRequestBodySize
	case maxBody < 0:
		maxBody = -1
	}
	return maxHeader, maxBody
}

func (s *Server) h1Config(xcfg *exchangeConfig, maxHeader int, maxBody int64) *h1.Config {
	return &h1.Config{
		Acquire:            func() exchange.Exchange { return acquireExchange(xcfg) },
		ReadHeaderTimeout:  timeoutOr(s.ReadHeaderTimeout, DefaultReadHeaderTimeout),
		ReadTimeout:        timeoutOr(s.ReadTimeout, 0),
		WriteTimeout:       timeoutOr(s.WriteTimeout, 0),
		IdleTimeout:        timeoutOr(s.IdleTimeout, DefaultIdleTimeout),
		MaxHeaderBytes:     maxHeader,
		MaxRequestBodySize: maxBody,
		ReduceMemoryUsage:  s.ReduceMemoryUsage,
		ShuttingDown:       &s.shuttingDown,
		Logger:             xcfg.logger,
	}
}

func timeoutOr(d, def time.Duration) time.Duration {
	switch {
	case d == 0:
		return def
	case d < 0:
		return 0
	}
	return d
}

func (s *Server) logger() *slog.Logger {
	if s.ErrorLog != nil {
		return s.ErrorLog
	}
	return slog.Default()
}

func (s *Server) trackListener(ln io.Closer) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.shuttingDown.Load() {
		return false
	}
	if s.listeners == nil {
		s.listeners = make(map[io.Closer]struct{})
	}
	s.listeners[ln] = struct{}{}
	return true
}

func (s *Server) untrackListener(ln io.Closer) {
	s.mu.Lock()
	delete(s.listeners, ln)
	s.mu.Unlock()
	_ = ln.Close()
}

func (s *Server) closeListeners() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var errs []error
	for ln := range s.listeners {
		if err := ln.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			errs = append(errs, err)
		}
		delete(s.listeners, ln)
	}
	return errors.Join(errs...)
}

// serverConn is a connection the server can close on Shutdown or Close.
type serverConn interface {
	CloseIfIdle() bool
	Close()
}

// handshakingConn is a connection whose protocol is not known yet.
type handshakingConn struct{ nc net.Conn }

func (handshakingConn) CloseIfIdle() bool { return false }
func (c handshakingConn) Close()          { _ = c.nc.Close() }

func (s *Server) trackConn(c serverConn) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.shuttingDown.Load() {
		return false
	}
	if s.conns == nil {
		s.conns = make(map[serverConn]struct{})
	}
	s.conns[c] = struct{}{}
	return true
}

// replaceConn swaps a connection for its protocol implementation. It reports false if the server
// started shutting down in between, in which case the caller must not serve it.
func (s *Server) replaceConn(old, c serverConn) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.conns, old)
	if s.shuttingDown.Load() {
		return false
	}
	s.conns[c] = struct{}{}
	return true
}

func (s *Server) untrackConn(c serverConn) {
	s.mu.Lock()
	delete(s.conns, c)
	s.mu.Unlock()
}

// closeIdleConns closes idle connections and reports whether no connections remain.
func (s *Server) closeIdleConns() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for c := range s.conns {
		c.CloseIfIdle()
	}
	return len(s.conns) == 0
}

func (s *Server) cancelContexts() {
	s.mu.Lock()
	cancels := s.cancels
	s.cancels = nil
	s.mu.Unlock()
	for _, cancel := range cancels {
		cancel()
	}
}
