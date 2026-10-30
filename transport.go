package qilat

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"net/url"
	"slices"
	"sync"
	"time"

	"github.com/erizeta/qilat/internal/bytesconv"
	"github.com/erizeta/qilat/internal/exchange"
	"github.com/erizeta/qilat/internal/h1"
	"github.com/erizeta/qilat/internal/h2"
	"github.com/erizeta/qilat/internal/h3"
)

// Defaults used when the corresponding Transport field is zero.
const (
	DefaultMaxIdleConnsPerHost = 100
	DefaultIdleConnTimeout     = 90 * time.Second
	DefaultDialTimeout         = 30 * time.Second
	DefaultTLSHandshakeTimeout = 10 * time.Second
	DefaultMaxResponseHeader   = 64 << 10
	DefaultHTTP3DialTimeout    = time.Second
)

// http3BrokenFor is how long a host whose advertised HTTP/3 endpoint failed is reached over TCP only.
const http3BrokenFor = 5 * time.Minute

// DefaultTransport is the Transport used by a Client whose Transport is nil.
var DefaultTransport = &Transport{}

// Transport is a [RoundTripper] that keeps connections open for reuse. HTTPS requests use HTTP/2 when the
// server offers it through ALPN, multiplexed on one connection per host; other requests use a pool of
// HTTP/1.1 connections. A zero Transport is ready to use, and safe for concurrent use.
//
// Like the [Server], zero fields mean safe defaults rather than "unlimited".
type Transport struct {
	// DialContext opens connections; nil means a net.Dialer. It is called with network "tcp", and with "udp"
	// for HTTP/3, where returning an error makes the request fall back to TCP.
	DialContext func(ctx context.Context, network, addr string) (net.Conn, error)
	// DialTimeout limits connecting; zero means DefaultDialTimeout.
	DialTimeout time.Duration
	// TLSClientConfig configures TLS for HTTPS; nil means the defaults. It is cloned for each connection.
	TLSClientConfig *tls.Config
	// TLSHandshakeTimeout limits the TLS handshake; zero means DefaultTLSHandshakeTimeout.
	TLSHandshakeTimeout time.Duration
	// Proxy returns the proxy for a request, or nil for none; nil means no proxy. HTTPS requests go through
	// an HTTP CONNECT tunnel, plain HTTP requests are sent to the proxy in absolute form. See ProxyFromEnvironment.
	Proxy func(req *Request) (*url.URL, error)
	// MaxConnsPerHost limits HTTP/1.1 connections per host, counting those in use; zero means no limit.
	// Requests wait for a free connection.
	MaxConnsPerHost int
	// MaxIdleConnsPerHost limits idle HTTP/1.1 connections kept per host; zero means DefaultMaxIdleConnsPerHost.
	// A negative value disables keep-alive.
	MaxIdleConnsPerHost int
	// IdleConnTimeout closes connections idle for this long; zero means DefaultIdleConnTimeout.
	IdleConnTimeout time.Duration
	// ResponseHeaderTimeout limits waiting for the response header after the request is sent; zero means no limit.
	ResponseHeaderTimeout time.Duration
	// MaxResponseHeaderBytes limits the response header; zero means DefaultMaxResponseHeader.
	MaxResponseHeaderBytes int
	// MaxResponseBodySize limits Response.ReadBody; zero means DefaultMaxResponseBodySize, negative no limit.
	// Reading Response.Body directly is not limited.
	MaxResponseBodySize int64
	// DisableHTTP2 keeps HTTPS requests on HTTP/1.1.
	DisableHTTP2 bool
	// H2C sends plain http:// requests over HTTP/2 with prior knowledge (RFC 9113 section 3.3), for servers
	// known to accept it, such as a qilat Server with H2C set. It is not used through a proxy.
	H2C bool
	// EnableHTTP3 sends HTTPS requests over HTTP/3 to hosts that advertised it with Alt-Svc in an earlier
	// response. When QUIC cannot connect within HTTP3DialTimeout, as when UDP is blocked, the request falls
	// back to TCP and the host is reached over TCP for a while. HTTP/3 is not used through a proxy.
	EnableHTTP3 bool
	// HTTP3DialTimeout limits the QUIC handshake before falling back to TCP; zero means DefaultHTTP3DialTimeout.
	HTTP3DialTimeout time.Duration

	initOnce sync.Once
	h1cfg    h1.ClientConfig
	h2cfg    h2.ClientConfig
	h3cfg    h3.ClientConfig

	altMu  sync.Mutex
	altSvc map[string]*altSvcEntry // by origin authority

	mu     sync.Mutex
	hosts  map[connKey]*hostConns
	closed bool
}

// connKey identifies a pool of connections. Requests through a proxy have their own pools.
type connKey struct {
	scheme    string
	authority string
	proxy     string
}

func (t *Transport) init() {
	t.initOnce.Do(func() {
		t.h1cfg = h1.ClientConfig{
			MaxHeaderBytes:        orDefault(t.MaxResponseHeaderBytes, DefaultMaxResponseHeader),
			ResponseHeaderTimeout: t.ResponseHeaderTimeout,
		}
		t.h2cfg = h2.ClientConfig{
			MaxHeaderListSize:     uint32(t.h1cfg.MaxHeaderBytes), //nolint:gosec // bounded by the int above
			StreamWindow:          clientStreamWindow,
			ResponseHeaderTimeout: t.ResponseHeaderTimeout,
		}
		t.h3cfg = h3.ClientConfig{
			MaxFieldSectionSize:   t.h1cfg.MaxHeaderBytes,
			ResponseHeaderTimeout: t.ResponseHeaderTimeout,
			IdleTimeout:           orDefault(t.IdleConnTimeout, DefaultIdleConnTimeout),
		}
	})
}

// clientStreamWindow is the HTTP/2 receive window per stream: how much of an unread body may be buffered.
const clientStreamWindow = 4 << 20

func orDefault[T int | int64 | time.Duration](v, def T) T {
	if v == 0 {
		return def
	}
	return v
}

// RoundTrip sends req on a pooled connection and reads the response header into resp.
func (t *Transport) RoundTrip(req *Request, resp *Response) error {
	req.live()
	t.init()
	if req.uri.form != formAbsolute {
		return errClientURI
	}
	ctx := req.clientContext()
	if err := ctx.Err(); err != nil {
		return err
	}
	var proxy *url.URL
	if t.Proxy != nil {
		var err error
		if proxy, err = t.Proxy(req); err != nil {
			return err
		}
	}
	creq := exchange.ClientRequest{
		Method:        req.method,
		Scheme:        bytesconv.B2S(req.uri.Scheme()),
		Authority:     req.host,
		Target:        originForm(&req.uri),
		Header:        (*headerList)(&req.header),
		ContentLength: req.contentLength,
	}
	if proxy != nil && creq.Scheme == "http" {
		creq.Target = req.uri.Raw()
		creq.ProxyAuthorization = proxyAuthorization(proxy)
	}
	if _, ok := req.Body.(noBody); !ok {
		creq.Body = req.Body
	} else {
		creq.ContentLength = 0
	}
	resp.maxBody = orDefault(t.MaxResponseBodySize, DefaultMaxResponseBodySize)
	hc := t.hostConns(creq.Scheme, req.uri.Host(), proxy)
	useAltSvc := t.EnableHTTP3 && proxy == nil && creq.Scheme == "https"
	for attempt := 0; ; attempt++ {
		var err error
		if addr, ok := t.http3Addr(useAltSvc, hc.key.authority); ok {
			err = hc.roundTripH3(ctx, addr, &creq, resp)
			if errors.Is(err, errHTTP3Dial) {
				t.markHTTP3Broken(hc.key.authority)
				err = hc.roundTrip(ctx, &creq, resp)
			}
		} else {
			err = hc.roundTrip(ctx, &creq, resp)
		}
		if err == nil {
			if useAltSvc && resp.proto != "HTTP/3.0" {
				t.noteAltSvc(hc.key.authority, resp.header.Peek("alt-svc"))
			}
			return nil
		}
		// A request the server never acted on is sent once more on a fresh connection, if sending it twice
		// is harmless and its body can be replayed.
		if attempt > 0 || !errors.Is(err, exchange.ErrNotProcessed) || !idempotentMethod(req.method) || creq.Body != nil {
			return err
		}
		resp.reset()
		resp.maxBody = orDefault(t.MaxResponseBodySize, DefaultMaxResponseBodySize)
	}
}

func idempotentMethod(m string) bool {
	switch m {
	case "GET", "HEAD", "OPTIONS", "TRACE", "PUT", "DELETE":
		return true
	}
	return false
}

func (t *Transport) hostConns(scheme string, authority []byte, proxy *url.URL) *hostConns {
	var proxyStr string
	if proxy != nil {
		proxyStr = proxy.String()
	}
	key := connKey{scheme: scheme, authority: bytesconv.B2S(authority), proxy: proxyStr}
	t.mu.Lock()
	defer t.mu.Unlock()
	if hc, ok := t.hosts[key]; ok {
		return hc
	}
	key.scheme = string(scheme)
	key.authority = string(authority)
	if t.hosts == nil {
		t.hosts = make(map[connKey]*hostConns)
	}
	hc := &hostConns{t: t, key: key, proxy: proxy}
	switch {
	case t.H2C && scheme == "http" && proxy == nil:
		hc.proto = protoH2 // prior knowledge
	case scheme == "https" && !t.DisableHTTP2:
		hc.proto = protoUnknown // ALPN decides on the first connection
	default:
		hc.proto = protoH1
	}
	t.hosts[key] = hc
	return hc
}

// CloseIdleConnections closes connections that are not carrying a request.
func (t *Transport) CloseIdleConnections() {
	t.mu.Lock()
	hosts := make([]*hostConns, 0, len(t.hosts))
	for _, hc := range t.hosts {
		hosts = append(hosts, hc)
	}
	t.mu.Unlock()
	for _, hc := range hosts {
		hc.closeIdle()
	}
}

var errHTTP3Dial = errors.New("qilat: HTTP/3 connection failed")

// altSvcEntry is what a host advertised in Alt-Svc (RFC 7838).
type altSvcEntry struct {
	raw         []byte // the header value it was parsed from, so repeats need no parsing
	addr        string // host:port of the HTTP/3 endpoint
	expires     time.Time
	brokenUntil time.Time
}

// http3Addr returns the HTTP/3 endpoint advertised for authority, if it is fresh and not known to fail.
func (t *Transport) http3Addr(enabled bool, authority string) (string, bool) {
	if !enabled {
		return "", false
	}
	t.altMu.Lock()
	defer t.altMu.Unlock()
	e := t.altSvc[authority]
	if e == nil || e.addr == "" {
		return "", false
	}
	now := time.Now()
	if now.After(e.expires) {
		delete(t.altSvc, authority)
		return "", false
	}
	return e.addr, now.After(e.brokenUntil)
}

func (t *Transport) markHTTP3Broken(authority string) {
	t.altMu.Lock()
	if e := t.altSvc[authority]; e != nil {
		e.brokenUntil = time.Now().Add(http3BrokenFor)
	}
	t.altMu.Unlock()
}

// noteAltSvc updates the cache from a response's Alt-Svc value. A missing value leaves the cache alone,
// as most responses repeat the header only occasionally; "clear" forgets the host.
func (t *Transport) noteAltSvc(authority string, v []byte) {
	if len(v) == 0 {
		return
	}
	t.altMu.Lock()
	defer t.altMu.Unlock()
	e := t.altSvc[authority]
	if e != nil && string(e.raw) == string(v) {
		e.expires = time.Now().Add(e.maxAgeFromRaw())
		return
	}
	addr, maxAge, ok := parseAltSvc(v, authority)
	if !ok {
		delete(t.altSvc, authority)
		return
	}
	if t.altSvc == nil {
		t.altSvc = make(map[string]*altSvcEntry)
	}
	if e == nil {
		e = new(altSvcEntry)
		t.altSvc[authority] = e
	}
	e.raw = append(e.raw[:0], v...)
	e.addr = addr
	e.expires = time.Now().Add(maxAge)
}

func (e *altSvcEntry) maxAgeFromRaw() time.Duration {
	_, ma, _ := parseAltSvc(e.raw, "")
	return ma
}

// parseAltSvc returns the first "h3" alternative in an Alt-Svc value as host:port, and its max age
// (24 hours by default). A value without one, or "clear", reports false.
func parseAltSvc(v []byte, origin string) (addr string, maxAge time.Duration, ok bool) {
	for len(v) > 0 {
		var alt []byte
		alt, v, _ = cutByte(v, ',')
		alt = trimOWS(alt)
		proto, rest, found := cutByte(alt, '=')
		if !found || string(trimOWS(proto)) != "h3" {
			continue
		}
		authority, params, _ := cutByte(rest, ';')
		authority = trimOWS(authority)
		if len(authority) < 2 || authority[0] != '"' || authority[len(authority)-1] != '"' {
			continue
		}
		host, port, err := net.SplitHostPort(string(authority[1 : len(authority)-1]))
		if err != nil || port == "" {
			continue
		}
		if host == "" {
			host = hostOnly(origin)
		}
		maxAge = 24 * time.Hour
		for len(params) > 0 {
			var p []byte
			p, params, _ = cutByte(params, ';')
			if k, val, _ := cutByte(trimOWS(p), '='); string(k) == "ma" {
				if n, ok := bytesconv.ParseUint(val); ok {
					maxAge = time.Duration(n) * time.Second
				}
			}
		}
		return net.JoinHostPort(host, port), maxAge, true
	}
	return "", 0, false
}

// roundTripH3 sends the request over HTTP/3, failing with errHTTP3Dial when no connection can be made.
func (hc *hostConns) roundTripH3(ctx context.Context, addr string, req *exchange.ClientRequest, resp *Response) error {
	for {
		c, err := hc.getH3(ctx, addr)
		if err != nil {
			return err
		}
		err = c.RoundTrip(ctx, req, (*clientResponse)(resp))
		if errors.Is(err, h3.ErrConnUnavailable) {
			hc.dropH3(c)
			continue
		}
		return err
	}
}

func (hc *hostConns) getH3(ctx context.Context, addr string) (*h3.ClientConn, error) {
	for {
		hc.mu.Lock()
		if c := hc.h3; c != nil && c.CanTakeRequest() {
			hc.mu.Unlock()
			return c, nil
		}
		if dialing := hc.h3Dial; dialing != nil {
			hc.mu.Unlock()
			select {
			case <-dialing:
				continue
			case <-ctx.Done():
				return nil, context.Cause(ctx)
			}
		}
		done := make(chan struct{})
		hc.h3Dial = done
		hc.mu.Unlock()

		t := hc.t
		dctx, cancel := context.WithTimeout(ctx, orDefault(t.HTTP3DialTimeout, DefaultHTTP3DialTimeout))
		c, err := h3.Dial(dctx, addr, t.tlsConfig(hc.key.authority), &t.h3cfg, t.DialContext, hc.dropH3)
		cancel()
		hc.mu.Lock()
		hc.h3Dial = nil
		if err == nil {
			hc.h3 = c
			hc.startTimerLocked()
		}
		hc.mu.Unlock()
		close(done)
		if err != nil {
			if ctx.Err() != nil {
				return nil, context.Cause(ctx)
			}
			return nil, errors.Join(errHTTP3Dial, err)
		}
		return c, nil
	}
}

func (hc *hostConns) dropH3(c *h3.ClientConn) {
	hc.mu.Lock()
	if hc.h3 == c {
		hc.h3 = nil
	}
	hc.mu.Unlock()
}

// hostConns holds the connections to one host.
type hostConns struct {
	t     *Transport
	key   connKey
	proxy *url.URL

	mu      sync.Mutex
	idle    []idleConn // most recently used last
	total   int        // HTTP/1.1 connections open or being dialed
	waiters []chan struct{}
	timer   *time.Timer
	h2      []*h2.ClientConn
	proto   hostProto
	dialing *sharedDial // a dial other requests wait for instead of dialing their own
	h3      *h3.ClientConn
	h3Dial  chan struct{}
}

// hostProto is what a host is known to speak over TCP.
type hostProto uint8

const (
	protoUnknown hostProto = iota // HTTPS before the first handshake
	protoH1
	protoH2
)

// sharedDial is a connection being opened while the host may speak HTTP/2, whose one connection would
// serve every request waiting for it.
type sharedDial struct {
	done   chan struct{}
	failed bool // set before done is closed
}

type idleConn struct {
	c     *h1.ClientConn
	since time.Time
}

func (hc *hostConns) roundTrip(ctx context.Context, req *exchange.ClientRequest, resp *Response) error {
	for {
		c1, c2, err := hc.getConn(ctx)
		if err != nil {
			return err
		}
		if c1 != nil {
			return c1.RoundTrip(ctx, req, (*clientResponse)(resp))
		}
		err = c2.RoundTrip(ctx, req, (*clientResponse)(resp))
		// Another request took the last free stream first: pick or dial another connection.
		if errors.Is(err, h2.ErrConnUnavailable) {
			continue
		}
		return err
	}
}

// getConn returns an HTTP/2 connection with a free stream, an idle HTTP/1.1 connection, or a new connection,
// waiting while MaxConnsPerHost HTTP/1.1 connections are in use.
func (hc *hostConns) getConn(ctx context.Context) (*h1.ClientConn, *h2.ClientConn, error) {
	limit := hc.t.MaxConnsPerHost
	share := true
	for {
		hc.mu.Lock()
		for _, c := range hc.h2 {
			if c.CanTakeRequest() {
				hc.mu.Unlock()
				return nil, c, nil
			}
		}
		if sd := hc.dialing; sd != nil && share {
			hc.mu.Unlock()
			select {
			case <-sd.done:
				// After a failed dial each waiter dials for itself, so one failure neither fails them all
				// nor makes them retry one after another.
				share = !sd.failed
				continue
			case <-ctx.Done():
				return nil, nil, context.Cause(ctx)
			}
		}
		if n := len(hc.idle); n > 0 {
			c := hc.idle[n-1].c
			hc.idle[n-1] = idleConn{}
			hc.idle = hc.idle[:n-1]
			hc.mu.Unlock()
			return c, nil, nil
		}
		if limit <= 0 || hc.total < limit || hc.proto == protoH2 {
			hc.total++
			var sd *sharedDial
			if share && hc.proto != protoH1 && hc.dialing == nil {
				// Requests that arrive meanwhile wait for this connection instead of dialing their own.
				// For a host that turns out to speak only HTTP/1.1 this delays its first burst by one
				// handshake; after that the host is known and nobody waits.
				sd = &sharedDial{done: make(chan struct{})}
				hc.dialing = sd
			}
			hc.mu.Unlock()
			c1, c2, err := hc.dialConn(ctx)
			if sd != nil {
				hc.mu.Lock()
				hc.dialing = nil
				sd.failed = err != nil
				hc.mu.Unlock()
				close(sd.done)
			}
			return c1, c2, err
		}
		ch := make(chan struct{}, 1)
		hc.waiters = append(hc.waiters, ch)
		hc.mu.Unlock()
		select {
		case <-ch:
		case <-ctx.Done():
			hc.mu.Lock()
			for i, w := range hc.waiters {
				if w == ch {
					hc.waiters = append(hc.waiters[:i], hc.waiters[i+1:]...)
					break
				}
			}
			hc.mu.Unlock()
			// A wakeup that raced with cancellation goes to the next waiter.
			select {
			case <-ch:
				hc.wakeOne()
			default:
			}
			return nil, nil, context.Cause(ctx)
		}
	}
}

// dialConn opens a connection counted in total and wraps it for the protocol the server chose.
func (hc *hostConns) dialConn(ctx context.Context) (*h1.ClientConn, *h2.ClientConn, error) {
	nc, err := hc.dial(ctx)
	if err != nil {
		hc.connGone()
		return nil, nil, err
	}
	tc, isTLS := nc.(*tls.Conn)
	h2c := !isTLS && hc.t.H2C && hc.proxy == nil
	if h2c || isTLS && tc.ConnectionState().NegotiatedProtocol == "h2" {
		c, err := h2.NewClientConn(nc, &hc.t.h2cfg, hc.removeH2)
		hc.connGone() // HTTP/2 connections do not count toward MaxConnsPerHost
		if err != nil {
			_ = nc.Close()
			return nil, nil, err
		}
		hc.mu.Lock()
		hc.h2 = append(hc.h2, c)
		hc.proto = protoH2
		hc.startTimerLocked()
		hc.mu.Unlock()
		return nil, c, nil
	}
	hc.mu.Lock()
	hc.proto = protoH1
	hc.mu.Unlock()
	return h1.NewClientConn(nc, &hc.t.h1cfg, hc.release), nil, nil
}

func (hc *hostConns) removeH2(c *h2.ClientConn) {
	hc.mu.Lock()
	for i, x := range hc.h2 {
		if x == c {
			hc.h2 = slices.Delete(hc.h2, i, i+1)
			break
		}
	}
	hc.mu.Unlock()
}

func (hc *hostConns) startTimerLocked() {
	if hc.timer == nil {
		hc.timer = time.AfterFunc(orDefault(hc.t.IdleConnTimeout, DefaultIdleConnTimeout), hc.expireIdle)
	}
}

// release takes a connection back after a response, keeping it idle if it can be reused.
func (hc *hostConns) release(c *h1.ClientConn, reusable bool) {
	maxIdle := orDefault(hc.t.MaxIdleConnsPerHost, DefaultMaxIdleConnsPerHost)
	hc.mu.Lock()
	if reusable && maxIdle > 0 && len(hc.idle) < maxIdle && !hc.t.isClosed() {
		hc.idle = append(hc.idle, idleConn{c: c, since: time.Now()})
		hc.startTimerLocked()
		hc.mu.Unlock()
		hc.wakeOne()
		return
	}
	hc.mu.Unlock()
	_ = c.Close()
	hc.connGone()
}

// connGone accounts for a connection that was closed or never opened.
func (hc *hostConns) connGone() {
	hc.mu.Lock()
	hc.total--
	hc.mu.Unlock()
	hc.wakeOne()
}

func (hc *hostConns) wakeOne() {
	hc.mu.Lock()
	if len(hc.waiters) > 0 {
		ch := hc.waiters[0]
		hc.waiters = hc.waiters[1:]
		ch <- struct{}{}
	}
	hc.mu.Unlock()
}

// expireIdle closes connections idle longer than IdleConnTimeout and reschedules itself while any remain.
func (hc *hostConns) expireIdle() {
	timeout := orDefault(hc.t.IdleConnTimeout, DefaultIdleConnTimeout)
	now := time.Now()
	var expired []*h1.ClientConn
	hc.mu.Lock()
	keep := hc.idle[:0]
	for _, ic := range hc.idle {
		if now.Sub(ic.since) >= timeout {
			expired = append(expired, ic.c)
		} else {
			keep = append(keep, ic)
		}
	}
	clear(hc.idle[len(keep):])
	hc.idle = keep
	hc.total -= len(expired)
	var idleH2 []*h2.ClientConn
	for _, c := range hc.h2 {
		if c.IdleFor(now) >= timeout {
			idleH2 = append(idleH2, c)
		}
	}
	h3c := hc.h3
	h3Idle := h3c != nil && h3c.IdleFor(now) >= timeout
	switch {
	case len(keep) > 0:
		hc.timer.Reset(timeout - now.Sub(keep[0].since))
	case len(hc.h2) > len(idleH2), h3c != nil && !h3Idle:
		hc.timer.Reset(timeout)
	default:
		hc.timer = nil
	}
	hc.mu.Unlock()
	if h3Idle {
		h3c.CloseIfIdle()
	}
	for _, c := range expired {
		_ = c.Close()
		hc.wakeOne()
	}
	for _, c := range idleH2 {
		c.CloseIfIdle()
	}
}

func (hc *hostConns) closeIdle() {
	hc.mu.Lock()
	idle := hc.idle
	hc.idle = nil
	hc.total -= len(idle)
	if hc.timer != nil {
		hc.timer.Stop()
		hc.timer = nil
	}
	conns := append([]*h2.ClientConn(nil), hc.h2...)
	h3c := hc.h3
	hc.mu.Unlock()
	if h3c != nil {
		h3c.CloseIfIdle()
	}
	for _, ic := range idle {
		_ = ic.c.Close()
	}
	for _, c := range conns {
		c.CloseIfIdle()
	}
}

func (t *Transport) isClosed() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.closed
}

// dial opens a connection to the host, through the proxy if there is one, with TLS for HTTPS.
func (hc *hostConns) dial(ctx context.Context) (net.Conn, error) {
	t := hc.t
	dctx, cancel := context.WithTimeout(ctx, orDefault(t.DialTimeout, DefaultDialTimeout))
	defer cancel()
	addr := hostPort(hc.key.authority, hc.key.scheme)
	target := addr
	if hc.proxy != nil {
		target = hostPort(hc.proxy.Host, hc.proxy.Scheme)
	}
	var nc net.Conn
	var err error
	if t.DialContext != nil {
		nc, err = t.DialContext(dctx, "tcp", target)
	} else {
		var d net.Dialer
		nc, err = d.DialContext(dctx, "tcp", target)
	}
	if err != nil {
		return nil, err
	}
	if hc.proxy != nil && hc.key.scheme == "https" {
		if err := connectTunnel(dctx, nc, hc.proxy, addr); err != nil {
			_ = nc.Close()
			return nil, err
		}
	}
	if hc.key.scheme != "https" {
		return nc, nil
	}
	tc := tls.Client(nc, t.tlsConfig(hc.key.authority))
	hctx, hcancel := context.WithTimeout(ctx, orDefault(t.TLSHandshakeTimeout, DefaultTLSHandshakeTimeout))
	defer hcancel()
	if err := tc.HandshakeContext(hctx); err != nil {
		_ = nc.Close()
		return nil, err
	}
	return tc, nil
}

func (t *Transport) tlsConfig(authority string) *tls.Config {
	var cfg *tls.Config
	if t.TLSClientConfig != nil {
		cfg = t.TLSClientConfig.Clone()
	} else {
		cfg = new(tls.Config)
	}
	if cfg.ServerName == "" {
		cfg.ServerName = hostOnly(authority)
	}
	if len(cfg.NextProtos) == 0 {
		if t.DisableHTTP2 {
			cfg.NextProtos = []string{"http/1.1"}
		} else {
			cfg.NextProtos = []string{"h2", "http/1.1"}
		}
	}
	return cfg
}

// originForm returns the path and query of an absolute URI, which follow its authority in the raw bytes.
func originForm(u *URI) []byte {
	raw := u.Raw()
	rest := raw[len(u.Scheme())+len("://")+len(u.Host()):]
	if len(rest) > 0 && rest[0] == '/' {
		return rest
	}
	return append([]byte{'/'}, rest...)
}

// hostPort adds the scheme's default port to authority when it has none.
func hostPort(authority, scheme string) string {
	if _, _, err := net.SplitHostPort(authority); err == nil {
		return authority
	}
	port := "80"
	if scheme == "https" {
		port = "443"
	}
	return net.JoinHostPort(trimBrackets(authority), port)
}

func hostOnly(authority string) string {
	if h, _, err := net.SplitHostPort(authority); err == nil {
		return h
	}
	return trimBrackets(authority)
}

func trimBrackets(h string) string {
	if len(h) > 1 && h[0] == '[' && h[len(h)-1] == ']' {
		return h[1 : len(h)-1]
	}
	return h
}
