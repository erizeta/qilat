package qilat

import (
	"context"
	"crypto/tls"
	"errors"
	"log/slog"
	"math"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/erizeta/qilat/internal/exchange"
	"github.com/erizeta/qilat/internal/h1"
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

	// ReduceMemoryUsage returns read buffers to a pool while connections are idle, costing one extra read per request.
	ReduceMemoryUsage bool
	// DebugPoison makes handlers that use a request or ResponseWriter after ServeHTTP returns panic. It disables pooling.
	DebugPoison bool

	// BaseContext returns the base context for requests from ln. If nil, context.Background is used.
	BaseContext func(ln net.Listener) context.Context
	// ErrorLog receives handler panics and connection errors. If nil, slog.Default is used.
	ErrorLog *slog.Logger

	mu           sync.Mutex
	listeners    map[net.Listener]struct{}
	conns        map[*h1.Conn]struct{}
	cancels      []context.CancelFunc
	shuttingDown atomic.Bool
}

// ListenAndServe listens on the TCP address addr and serves HTTP/1.1. An empty addr means ":http".
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

// ListenAndServeTLS listens on addr and serves HTTPS. An empty addr means ":https". The certificate files
// may be empty if TLSConfig already holds certificates.
func (s *Server) ListenAndServeTLS(addr, certFile, keyFile string) error {
	if s.shuttingDown.Load() {
		return ErrServerClosed
	}
	if addr == "" {
		addr = ":https"
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	return s.ServeTLS(ln, certFile, keyFile)
}

// ServeTLS serves HTTPS on ln. See [Server.ListenAndServeTLS] for the certificate arguments.
func (s *Server) ServeTLS(ln net.Listener, certFile, keyFile string) error {
	var config *tls.Config
	if s.TLSConfig != nil {
		config = s.TLSConfig.Clone()
	} else {
		config = &tls.Config{MinVersion: tls.VersionTLS12}
	}
	if len(config.NextProtos) == 0 {
		config.NextProtos = []string{"http/1.1"}
	}
	if certFile != "" || keyFile != "" || (len(config.Certificates) == 0 && config.GetCertificate == nil) {
		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			_ = ln.Close()
			return err
		}
		config.Certificates = append(config.Certificates, cert)
	}
	return s.Serve(tls.NewListener(ln, config))
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

	xcfg := &exchangeConfig{handler: s.Handler, baseCtx: ctx, poison: s.DebugPoison, logger: s.logger()}
	cfg := s.h1Config(xcfg)

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
		c := h1.NewConn(nc, cfg)
		if !s.trackConn(c) {
			_ = nc.Close()
			if sem != nil {
				<-sem
			}
			continue
		}
		go func() {
			defer func() {
				s.untrackConn(c)
				if sem != nil {
					<-sem
				}
			}()
			c.Serve()
		}()
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

func (s *Server) h1Config(xcfg *exchangeConfig) *h1.Config {
	maxHeader := s.MaxHeaderBytes
	switch {
	case maxHeader == 0:
		maxHeader = DefaultMaxHeaderBytes
	case maxHeader < 0:
		maxHeader = math.MaxInt
	}
	maxBody := s.MaxRequestBodySize
	switch {
	case maxBody == 0:
		maxBody = DefaultMaxRequestBodySize
	case maxBody < 0:
		maxBody = -1
	}
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

func (s *Server) trackListener(ln net.Listener) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.shuttingDown.Load() {
		return false
	}
	if s.listeners == nil {
		s.listeners = make(map[net.Listener]struct{})
	}
	s.listeners[ln] = struct{}{}
	return true
}

func (s *Server) untrackListener(ln net.Listener) {
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

func (s *Server) trackConn(c *h1.Conn) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.shuttingDown.Load() {
		return false
	}
	if s.conns == nil {
		s.conns = make(map[*h1.Conn]struct{})
	}
	s.conns[c] = struct{}{}
	return true
}

func (s *Server) untrackConn(c *h1.Conn) {
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
