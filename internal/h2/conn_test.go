package h2

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/erizeta/qilat/internal/exchange"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/hpack"
)

// fakeExchange is a minimal core: it records the request and runs serve.
type fakeExchange struct {
	method, target, host string
	header               [][2]string
	body                 io.Reader
	contentLength        int64
	ctx                  context.Context
	cancel               context.CancelFunc
	serve                func(x *fakeExchange, w exchange.ResponseSink) error
	released             *atomic.Int64
}

func (x *fakeExchange) SetMethod(m []byte) error { x.method = string(m); return nil }
func (x *fakeExchange) SetTarget(t []byte) error { x.target = string(t); return nil }
func (x *fakeExchange) SetHost(h []byte) error   { x.host = string(h); return nil }
func (x *fakeExchange) SetProto(int, int)        {}
func (x *fakeExchange) AddHeader(n, v []byte) error {
	x.header = append(x.header, [2]string{string(n), string(v)})
	return nil
}
func (x *fakeExchange) SetBody(b io.Reader, n int64)                    { x.body, x.contentLength = b, n }
func (x *fakeExchange) SetConn(net.Addr, *tls.ConnectionState)          {}
func (x *fakeExchange) SetDisconnectWatcher(exchange.DisconnectWatcher) {}
func (x *fakeExchange) Serve(w exchange.ResponseSink) error             { return x.serve(x, w) }
func (x *fakeExchange) Cancel()                                         { x.cancel() }
func (x *fakeExchange) Release()                                        { x.cancel(); x.released.Add(1) }
func (x *fakeExchange) headerValue(name string) string                  { return lookup(x.header, name) }
func lookup(h [][2]string, name string) string {
	for _, f := range h {
		if f[0] == name {
			return f[1]
		}
	}
	return ""
}

type fields [][2]string

func (f fields) Len() int                      { return len(f) }
func (f fields) At(i int) (name, value []byte) { return []byte(f[i][0]), []byte(f[i][1]) }

// okServe answers every request with a small body.
func okServe(x *fakeExchange, w exchange.ResponseSink) error {
	if x.body != nil {
		_, _ = io.Copy(io.Discard, x.body)
	}
	if err := w.WriteHeader(200, fields{{"content-type", "text/plain"}}, 2); err != nil {
		return err
	}
	if _, err := w.Write([]byte("ok")); err != nil {
		return err
	}
	return w.Finish()
}

type testConn struct {
	t        *testing.T
	cc       net.Conn
	fr       *http2.Framer
	enc      *hpack.Encoder
	encBuf   bytes.Buffer
	done     chan struct{}
	writes   chan func() error
	released atomic.Int64
	conn     *Conn
}

func testConfig() *Config {
	return &Config{
		MaxConcurrentStreams: 100,
		MaxReadFrameSize:     1 << 14,
		MaxHeaderListSize:    16 << 10,
		HeaderTableSize:      4096,
		StreamWindow:         1 << 16,
		ConnWindow:           1 << 20,
		MaxRequestBodySize:   1 << 20,
		ReadHeaderTimeout:    5 * time.Second,
	}
}

// startConn serves one HTTP/2 connection over a pipe and performs the client handshake.
func startConn(t *testing.T, serve func(*fakeExchange, exchange.ResponseSink) error, cfg *Config) *testConn {
	t.Helper()
	if cfg == nil {
		cfg = testConfig()
	}
	sc, cc := tcpPair(t)
	tc := &testConn{t: t, cc: cc, done: make(chan struct{}), writes: make(chan func() error, 64)}
	// net/http2's Framer is not safe for concurrent use, so client writes run in order on one goroutine.
	go func() {
		for f := range tc.writes {
			if f() != nil {
				for range tc.writes {
				}
				return
			}
		}
	}()
	cfg.Acquire = func() exchange.Exchange {
		ctx, cancel := context.WithCancel(context.Background())
		return &fakeExchange{serve: serve, ctx: ctx, cancel: cancel, released: &tc.released}
	}
	tc.conn = NewConn(sc, nil, nil, cfg)
	go func() {
		defer close(tc.done)
		tc.conn.Serve()
	}()
	t.Cleanup(func() {
		close(tc.writes)
		_ = cc.Close()
		select {
		case <-tc.done:
		case <-time.After(5 * time.Second):
			t.Error("connection did not shut down")
		}
	})
	_ = cc.SetDeadline(time.Now().Add(10 * time.Second))
	tc.fr = http2.NewFramer(cc, cc)
	tc.fr.ReadMetaHeaders = hpack.NewDecoder(4096, nil)
	tc.enc = hpack.NewEncoder(&tc.encBuf)
	if _, err := io.WriteString(cc, ClientPreface); err != nil {
		t.Fatal(err)
	}
	tc.write(func() error { return tc.fr.WriteSettings() })
	f := tc.read()
	if _, ok := f.(*http2.SettingsFrame); !ok {
		t.Fatalf("first frame %T", f)
	}
	return tc
}

// tcpPair returns both ends of a loopback TCP connection. Unlike net.Pipe it buffers, so a test that is not
// reading does not block the server's writes.
func tcpPair(t testing.TB) (server, client net.Conn) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	accepted := make(chan net.Conn, 1)
	go func() {
		c, _ := ln.Accept()
		accepted <- c
	}()
	client, err = net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	server = <-accepted
	if server == nil {
		t.Fatal("accept failed")
	}
	return server, client
}

// read returns the next frame, skipping connection-level noise (SETTINGS, WINDOW_UPDATE on stream 0).
func (tc *testConn) read() http2.Frame {
	tc.t.Helper()
	for {
		f, err := tc.fr.ReadFrame()
		if err != nil {
			tc.t.Fatalf("ReadFrame: %v", err)
		}
		switch f := f.(type) {
		case *http2.SettingsFrame:
			if !f.IsAck() {
				return f
			}
			continue
		case *http2.WindowUpdateFrame:
			if f.StreamID == 0 {
				continue
			}
		}
		return f
	}
}

func (tc *testConn) headerBlock(fs ...string) []byte {
	tc.encBuf.Reset()
	for i := 0; i < len(fs); i += 2 {
		_ = tc.enc.WriteField(hpack.HeaderField{Name: fs[i], Value: fs[i+1]})
	}
	return bytes.Clone(tc.encBuf.Bytes())
}

func (tc *testConn) get(id uint32, end bool, extra ...string) {
	fs := append([]string{":method", "GET", ":scheme", "https", ":path", "/", ":authority", "example.com"}, extra...)
	tc.write(func() error {
		return tc.fr.WriteHeaders(http2.HeadersFrameParam{
			StreamID: id, BlockFragment: tc.headerBlock(fs...), EndStream: end, EndHeaders: true,
		})
	})
}

// write queues a client write.
func (tc *testConn) write(f func() error) { tc.writes <- f }

// expectGoAway reads until GOAWAY and checks its code.
func (tc *testConn) expectGoAway(code http2.ErrCode) {
	tc.t.Helper()
	for {
		f, err := tc.fr.ReadFrame()
		if err != nil {
			tc.t.Fatalf("waiting for GOAWAY %v: %v", code, err)
		}
		if ga, ok := f.(*http2.GoAwayFrame); ok {
			if ga.ErrCode != code {
				tc.t.Fatalf("GOAWAY %v, want %v", ga.ErrCode, code)
			}
			return
		}
	}
}

func TestRequestAndResponse(t *testing.T) {
	var got *fakeExchange
	tc := startConn(t, func(x *fakeExchange, w exchange.ResponseSink) error {
		got = x
		return okServe(x, w)
	}, nil)
	tc.get(1, true, "cookie", "a=1", "cookie", "b=2", "priority", "u=1, i", "x-custom", "v")
	h, ok := tc.read().(*http2.MetaHeadersFrame)
	if !ok || h.PseudoValue("status") != "200" || h.StreamEnded() {
		t.Fatalf("response headers: %#v", h)
	}
	d, ok := tc.read().(*http2.DataFrame)
	if !ok || string(d.Data()) != "ok" {
		t.Fatalf("data: %#v", d)
	}
	if end, ok := tc.read().(*http2.DataFrame); !ok || !end.StreamEnded() {
		t.Fatalf("end of stream: %#v", end)
	}
	if got.method != "GET" || got.target != "/" || got.host != "example.com" || got.headerValue("cookie") != "a=1; b=2" ||
		got.headerValue("x-custom") != "v" {
		t.Fatalf("request: %+v", got)
	}
}

func TestCompleteResponseEndsOnLastFrame(t *testing.T) {
	tc := startConn(t, func(x *fakeExchange, w exchange.ResponseSink) error {
		cs := w.(exchange.CompleteSink)
		return cs.WriteComplete(200, fields{{"content-length", "5"}}, 5, []byte("hello"), fields{{"grpc-status", "0"}})
	}, nil)
	tc.get(1, true)
	if h := tc.read().(*http2.MetaHeadersFrame); h.StreamEnded() || h.PseudoValue("status") != "200" {
		t.Fatalf("headers %v", h)
	}
	if d := tc.read().(*http2.DataFrame); string(d.Data()) != "hello" || d.StreamEnded() {
		t.Fatalf("data %v", d)
	}
	tr := tc.read().(*http2.MetaHeadersFrame)
	if !tr.StreamEnded() || len(tr.Fields) != 1 || tr.Fields[0].Name != "grpc-status" {
		t.Fatalf("trailers %v", tr.Fields)
	}
}

func TestMalformedRequestsAreReset(t *testing.T) {
	for _, tt := range []struct {
		name string
		fs   []string
	}{
		{"uppercase name", []string{":method", "GET", ":scheme", "https", ":path", "/", "X-Up", "v"}},
		{"connection header", []string{":method", "GET", ":scheme", "https", ":path", "/", "connection", "close"}},
		{"te gzip", []string{":method", "GET", ":scheme", "https", ":path", "/", "te", "gzip"}},
		{"missing path", []string{":method", "GET", ":scheme", "https"}},
		{"empty path", []string{":method", "GET", ":scheme", "https", ":path", ""}},
		{"relative path", []string{":method", "GET", ":scheme", "https", ":path", "x"}},
		{"pseudo after regular", []string{":method", "GET", "a", "b", ":scheme", "https", ":path", "/"}},
		{"duplicate pseudo", []string{":method", "GET", ":method", "GET", ":scheme", "https", ":path", "/"}},
		{"unknown pseudo", []string{":method", "GET", ":scheme", "https", ":path", "/", ":foo", "x"}},
		{"response pseudo", []string{":status", "200", ":method", "GET", ":scheme", "https", ":path", "/"}},
		{"connect with path", []string{":method", "CONNECT", ":authority", "a:443", ":path", "/"}},
		{"value with leading space", []string{":method", "GET", ":scheme", "https", ":path", "/", "a", " b"}},
		{"authority differs from host", []string{":method", "GET", ":scheme", "https", ":path", "/", ":authority", "a", "host", "b"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			tc := startConn(t, okServe, nil)
			tc.write(func() error {
				return tc.fr.WriteHeaders(http2.HeadersFrameParam{StreamID: 1, BlockFragment: tc.headerBlock(tt.fs...),
					EndStream: true, EndHeaders: true})
			})
			if rst, ok := tc.read().(*http2.RSTStreamFrame); !ok || rst.ErrCode != http2.ErrCodeProtocol {
				t.Fatalf("got %v, want RST_STREAM PROTOCOL_ERROR", rst)
			}
			// The connection, and its HPACK state, must still work.
			tc.get(3, true)
			if h, ok := tc.read().(*http2.MetaHeadersFrame); !ok || h.StreamID != 3 {
				t.Fatalf("next request: %v", h)
			}
		})
	}
}

func TestRequestBodyLimits(t *testing.T) {
	cfg := testConfig()
	cfg.MaxRequestBodySize = 10
	bodyErr := make(chan error, 1)
	tc := startConn(t, func(x *fakeExchange, w exchange.ResponseSink) error {
		_, err := io.ReadAll(x.body)
		bodyErr <- err
		return okServe(x, w)
	}, cfg)
	// A declared length over the limit is answered with 413 without running a handler.
	tc.get(1, false, "content-length", "11")
	if h := tc.read().(*http2.MetaHeadersFrame); h.PseudoValue("status") != "413" {
		t.Fatalf("status %s", h.PseudoValue("status"))
	}
	if d := tc.read().(*http2.DataFrame); !d.StreamEnded() {
		t.Fatal("413 body does not end the stream")
	}
	if rst := tc.read().(*http2.RSTStreamFrame); rst.ErrCode != http2.ErrCodeNo {
		t.Fatalf("RST_STREAM %v, want NO_ERROR so the client stops sending", rst.ErrCode)
	}
	// An undeclared body that grows too large fails the handler's read.
	tc.get(3, false)
	tc.write(func() error { return tc.fr.WriteData(3, true, make([]byte, 11)) })
	if err := <-bodyErr; !errors.Is(err, exchange.ErrBodyTooLarge) {
		t.Fatalf("body error %v", err)
	}
}

func TestContentLengthMismatch(t *testing.T) {
	tc := startConn(t, okServe, nil)
	tc.get(1, false, "content-length", "3")
	tc.write(func() error { return tc.fr.WriteData(1, true, []byte("toolong")) })
	for {
		if rst, ok := tc.read().(*http2.RSTStreamFrame); ok {
			if rst.ErrCode != http2.ErrCodeProtocol {
				t.Fatalf("RST_STREAM %v", rst.ErrCode)
			}
			return
		}
	}
}

func TestExpectContinue(t *testing.T) {
	tc := startConn(t, okServe, nil)
	tc.get(1, false, "expect", "100-continue")
	if h := tc.read().(*http2.MetaHeadersFrame); h.PseudoValue("status") != "100" {
		t.Fatalf("status %s, want 100", h.PseudoValue("status"))
	}
	tc.write(func() error { return tc.fr.WriteData(1, true, []byte("x")) })
	if h := tc.read().(*http2.MetaHeadersFrame); h.PseudoValue("status") != "200" {
		t.Fatalf("status %s", h.PseudoValue("status"))
	}
}

func TestPriorityOrdersWrites(t *testing.T) {
	var s writeSched
	urgent, normal, background := newWaiter(5, &atomic.Uint32{}), newWaiter(3, &atomic.Uint32{}), newWaiter(1, &atomic.Uint32{})
	urgent.priority.Store(0 << 1)
	normal.priority.Store(3 << 1)
	background.priority.Store(7<<1 | 1)
	s.lock(newWaiter(0, nil))
	var order []uint32
	var mu sync.Mutex
	var wg sync.WaitGroup
	for i, w := range []*waiter{background, normal, urgent} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.lock(w)
			mu.Lock()
			order = append(order, w.id)
			mu.Unlock()
			s.unlock()
		}()
		for !s.hasWaitersN(i + 1) {
			runtime.Gosched()
		}
	}
	s.unlock()
	wg.Wait()
	if order[0] != 5 || order[1] != 3 || order[2] != 1 {
		t.Fatalf("order %v, want [5 3 1]", order)
	}
}

func (s *writeSched) hasWaitersN(n int) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.waiters) >= n
}

func TestRapidResetIsBounded(t *testing.T) {
	release := make(chan struct{})
	var running atomic.Int64
	var peak atomic.Int64
	cfg := testConfig()
	cfg.MaxConcurrentStreams = 10
	tc := startConn(t, func(x *fakeExchange, w exchange.ResponseSink) error {
		n := running.Add(1)
		for p := peak.Load(); n > p && !peak.CompareAndSwap(p, n); {
			p = peak.Load()
		}
		<-release // handlers ignore cancellation, the worst case for Rapid Reset
		running.Add(-1)
		return okServe(x, w)
	}, cfg)
	defer close(release)
	tc.write(func() error {
		for id := uint32(1); id < 2000; id += 2 {
			block := tc.headerBlock(":method", "GET", ":scheme", "https", ":path", "/")
			if err := tc.fr.WriteHeaders(http2.HeadersFrameParam{StreamID: id, BlockFragment: block, EndStream: true, EndHeaders: true}); err != nil {
				return err
			}
			if err := tc.fr.WriteRSTStream(id, http2.ErrCodeCancel); err != nil {
				return err
			}
		}
		return nil
	})
	tc.expectGoAway(http2.ErrCodeEnhanceYourCalm)
	if p := peak.Load(); p > 10 {
		t.Fatalf("%d handlers ran at once, limit is 10", p)
	}
}

func TestContinuationFlood(t *testing.T) {
	tc := startConn(t, okServe, nil)
	tc.write(func() error {
		block := tc.headerBlock(":method", "GET", ":scheme", "https", ":path", "/")
		if err := tc.fr.WriteHeaders(http2.HeadersFrameParam{StreamID: 1, BlockFragment: block[:1]}); err != nil {
			return err
		}
		for range 10000 {
			if err := tc.fr.WriteContinuation(1, false, nil); err != nil {
				return err
			}
		}
		return nil
	})
	tc.expectGoAway(http2.ErrCodeEnhanceYourCalm)
}

func TestHeaderListTooLarge(t *testing.T) {
	cfg := testConfig()
	cfg.MaxHeaderListSize = 1024
	tc := startConn(t, okServe, cfg)
	tc.get(1, true, "x-big", strings.Repeat("a", 2000))
	if h := tc.read().(*http2.MetaHeadersFrame); h.PseudoValue("status") != "431" {
		t.Fatalf("status %s, want 431", h.PseudoValue("status"))
	}
}

func TestPingFlood(t *testing.T) {
	tc := startConn(t, okServe, nil)
	tc.write(func() error {
		for range 5000 {
			if err := tc.fr.WritePing(false, [8]byte{}); err != nil {
				return err
			}
		}
		return nil
	})
	tc.expectGoAway(http2.ErrCodeEnhanceYourCalm)
}

func TestStreamLimitRefused(t *testing.T) {
	cfg := testConfig()
	cfg.MaxConcurrentStreams = 1
	block := make(chan struct{})
	tc := startConn(t, func(x *fakeExchange, w exchange.ResponseSink) error {
		<-block
		return okServe(x, w)
	}, cfg)
	defer close(block)
	tc.get(1, true)
	tc.get(3, true)
	if rst, ok := tc.read().(*http2.RSTStreamFrame); !ok || rst.StreamID != 3 || rst.ErrCode != http2.ErrCodeRefusedStream {
		t.Fatalf("got %v, want RST_STREAM REFUSED_STREAM on 3", rst)
	}
}

func TestGracefulShutdown(t *testing.T) {
	proceed := make(chan struct{})
	tc := startConn(t, func(x *fakeExchange, w exchange.ResponseSink) error {
		<-proceed
		return okServe(x, w)
	}, nil)
	tc.get(1, true)
	for tc.conn.activeStreams() == 0 {
		runtime.Gosched()
	}
	closed := make(chan bool, 1)
	go func() { closed <- tc.conn.CloseIfIdle() }()
	ga := tc.read().(*http2.GoAwayFrame)
	if ga.ErrCode != http2.ErrCodeNo || ga.LastStreamID != 1 {
		t.Fatalf("GOAWAY %v last %d", ga.ErrCode, ga.LastStreamID)
	}
	if <-closed {
		t.Fatal("closed with an active stream")
	}
	close(proceed)
	if h := tc.read().(*http2.MetaHeadersFrame); h.StreamID != 1 {
		t.Fatalf("in-flight response: %v", h)
	}
	// After the last stream ends, the connection closes on its own.
	for {
		if _, err := tc.fr.ReadFrame(); err != nil {
			break
		}
	}
	<-tc.done
	if n := tc.released.Load(); n != 1 {
		t.Fatalf("released %d exchanges, want 1", n)
	}
}

func (c *Conn) activeStreams() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.streams)
}

func TestHPACKEncoderRoundTrip(t *testing.T) {
	enc := newEncoder(4096)
	dec := hpack.NewDecoder(4096, nil)
	type field struct{ name, value string }
	blocks := [][]field{
		{{":status", "200"}, {"content-type", "text/html; charset=utf-8"}, {"x-custom", strings.Repeat("v", 300)},
			{"x-empty", ""}, {"content-length", "12"}},
		{{":status", "418"}, {"content-type", "text/html; charset=utf-8"}, {"x-custom", strings.Repeat("v", 300)},
			{"content-length", "13"}},
	}
	var sizes []int
	for _, block := range blocks {
		b := enc.startBlock(nil)
		for _, f := range block {
			if f.name == ":status" {
				b = enc.appendStatus(b, map[string]int{"200": 200, "418": 418}[f.value])
			} else {
				b = enc.appendField(b, []byte(f.name), []byte(f.value))
			}
		}
		sizes = append(sizes, len(b))
		got, err := dec.DecodeFull(b)
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != len(block) {
			t.Fatalf("%d fields, want %d", len(got), len(block))
		}
		for i, f := range got {
			if f.Name != block[i].name || f.Value != block[i].value {
				t.Errorf("field %d = %s: %s, want %s: %s", i, f.Name, f.Value, block[i].name, block[i].value)
			}
		}
	}
	if sizes[1] > 20 {
		t.Errorf("repeated fields were not indexed: second block is %d bytes", sizes[1])
	}
	// The peer shrinks the table: the next block must announce it and still decode.
	enc.setMaxSize(0)
	dec.SetAllowedMaxDynamicTableSize(4096)
	b := enc.appendField(enc.startBlock(nil), []byte("x-custom"), []byte("v"))
	got, err := dec.DecodeFull(b)
	if err != nil || len(got) != 1 || got[0].Value != "v" || enc.size != 0 {
		t.Fatalf("after shrink: %v %v size %d", got, err, enc.size)
	}
}

func TestHPACKEncoderEviction(t *testing.T) {
	enc := newEncoder(256)
	dec := hpack.NewDecoder(256, nil)
	for i := range 2000 {
		name, value := []byte(fmt.Sprintf("x-h%d", i%37)), []byte(strings.Repeat("v", i%50))
		b := enc.appendField(enc.startBlock(nil), name, value)
		got, err := dec.DecodeFull(b)
		if err != nil || len(got) != 1 || got[0].Name != string(name) || got[0].Value != string(value) {
			t.Fatalf("iteration %d: %v %v", i, got, err)
		}
		if enc.size > enc.maxSize || len(enc.fields) > len(enc.entries)-enc.head || len(enc.entries) > 64 {
			t.Fatalf("iteration %d: size %d entries %d maps %d", i, enc.size, len(enc.entries)-enc.head, len(enc.fields))
		}
	}
}

func FuzzConn(f *testing.F) {
	var seed bytes.Buffer
	fr := http2.NewFramer(&seed, nil)
	_ = fr.WriteSettings(http2.Setting{ID: http2.SettingInitialWindowSize, Val: 10})
	var hb bytes.Buffer
	enc := hpack.NewEncoder(&hb)
	for _, kv := range [][2]string{{":method", "POST"}, {":scheme", "https"}, {":path", "/"}, {"content-length", "3"}} {
		_ = enc.WriteField(hpack.HeaderField{Name: kv[0], Value: kv[1]})
	}
	_ = fr.WriteHeaders(http2.HeadersFrameParam{StreamID: 1, BlockFragment: hb.Bytes(), EndHeaders: true})
	_ = fr.WriteData(1, true, []byte("abc"))
	_ = fr.WritePing(false, [8]byte{1})
	_ = fr.WriteWindowUpdate(0, 100)
	_ = fr.WriteRSTStream(1, http2.ErrCodeCancel)
	f.Add(seed.Bytes())
	f.Add([]byte{0, 0, 0, 4, 0, 0, 0, 0, 0})
	f.Fuzz(func(t *testing.T, data []byte) {
		cfg := testConfig()
		cfg.Acquire = func() exchange.Exchange {
			ctx, cancel := context.WithCancel(context.Background())
			return &fakeExchange{serve: okServe, ctx: ctx, cancel: cancel, released: new(atomic.Int64)}
		}
		sc, cc := net.Pipe()
		c := NewConn(sc, nil, nil, cfg)
		done := make(chan struct{})
		go func() {
			defer close(done)
			c.Serve()
		}()
		go func() { _, _ = io.Copy(io.Discard, cc) }()
		_ = cc.SetWriteDeadline(time.Now().Add(time.Second))
		_, _ = io.WriteString(cc, ClientPreface)
		_, _ = cc.Write([]byte{0, 0, 0, 4, 0, 0, 0, 0, 0}) // empty SETTINGS
		_, _ = cc.Write(data)
		_ = cc.Close()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("connection did not finish")
		}
	})
}

// expectRST reads until RST_STREAM for id and checks its code.
func (tc *testConn) expectRST(id uint32, code http2.ErrCode) {
	tc.t.Helper()
	for {
		f, err := tc.fr.ReadFrame()
		if err != nil {
			tc.t.Fatalf("waiting for RST_STREAM %v: %v", code, err)
		}
		switch f := f.(type) {
		case *http2.RSTStreamFrame:
			if f.StreamID == id {
				if f.ErrCode != code {
					tc.t.Fatalf("RST_STREAM %v, want %v", f.ErrCode, code)
				}
				return
			}
		case *http2.GoAwayFrame:
			tc.t.Fatalf("GOAWAY %v while waiting for RST_STREAM %v", f.ErrCode, code)
		}
	}
}

func raw(typ http2.FrameType, flags http2.Flags, id uint32, payload ...byte) func(*testConn) error {
	return func(tc *testConn) error { return tc.fr.WriteRawFrame(typ, flags, id, payload) }
}

func TestConnectionErrors(t *testing.T) {
	headers := func(id uint32) func(*testConn) error {
		return func(tc *testConn) error {
			return tc.fr.WriteHeaders(http2.HeadersFrameParam{StreamID: id, EndHeaders: true, EndStream: true,
				BlockFragment: tc.headerBlock(":method", "GET", ":scheme", "https", ":path", "/")})
		}
	}
	for _, tt := range []struct {
		name   string
		frames []func(*testConn) error
		want   http2.ErrCode
	}{
		{"DATA on stream 0", []func(*testConn) error{raw(http2.FrameData, 0, 0, 'x')}, http2.ErrCodeProtocol},
		{"DATA on idle stream", []func(*testConn) error{raw(http2.FrameData, 0, 5, 'x')}, http2.ErrCodeProtocol},
		{"HEADERS on even stream", []func(*testConn) error{headers(2)}, http2.ErrCodeProtocol},
		{"HEADERS on lower stream", []func(*testConn) error{headers(5), headers(3)}, http2.ErrCodeProtocol},
		{"SETTINGS on stream", []func(*testConn) error{raw(http2.FrameSettings, 0, 1)}, http2.ErrCodeProtocol},
		{"SETTINGS bad length", []func(*testConn) error{raw(http2.FrameSettings, 0, 0, 1, 2, 3, 4, 5)}, http2.ErrCodeFrameSize},
		{"SETTINGS ACK with payload", []func(*testConn) error{raw(http2.FrameSettings, http2.FlagSettingsAck, 0, 0, 1, 0, 0, 0, 0)}, http2.ErrCodeFrameSize},
		{"ENABLE_PUSH 2", []func(*testConn) error{raw(http2.FrameSettings, 0, 0, 0, 2, 0, 0, 0, 2)}, http2.ErrCodeProtocol},
		{"INITIAL_WINDOW_SIZE too large", []func(*testConn) error{raw(http2.FrameSettings, 0, 0, 0, 4, 0x80, 0, 0, 0)}, http2.ErrCodeFlowControl},
		{"MAX_FRAME_SIZE too small", []func(*testConn) error{raw(http2.FrameSettings, 0, 0, 0, 5, 0, 0, 0, 100)}, http2.ErrCodeProtocol},
		{"PING bad length", []func(*testConn) error{raw(http2.FramePing, 0, 0, 1, 2, 3, 4, 5, 6, 7)}, http2.ErrCodeFrameSize},
		{"PING on stream", []func(*testConn) error{raw(http2.FramePing, 0, 1, 1, 2, 3, 4, 5, 6, 7, 8)}, http2.ErrCodeProtocol},
		{"PUSH_PROMISE", []func(*testConn) error{raw(http2.FramePushPromise, 0, 1, 0, 0, 0, 2)}, http2.ErrCodeProtocol},
		{"RST_STREAM on stream 0", []func(*testConn) error{raw(http2.FrameRSTStream, 0, 0, 0, 0, 0, 8)}, http2.ErrCodeProtocol},
		{"RST_STREAM on idle stream", []func(*testConn) error{raw(http2.FrameRSTStream, 0, 7, 0, 0, 0, 8)}, http2.ErrCodeProtocol},
		{"RST_STREAM bad length", []func(*testConn) error{headers(1), raw(http2.FrameRSTStream, 0, 1, 0, 0, 8)}, http2.ErrCodeFrameSize},
		{"WINDOW_UPDATE zero", []func(*testConn) error{raw(http2.FrameWindowUpdate, 0, 0, 0, 0, 0, 0)}, http2.ErrCodeProtocol},
		{"WINDOW_UPDATE overflow", []func(*testConn) error{raw(http2.FrameWindowUpdate, 0, 0, 0x7f, 0xff, 0xff, 0xff)}, http2.ErrCodeFlowControl},
		{"WINDOW_UPDATE bad length", []func(*testConn) error{raw(http2.FrameWindowUpdate, 0, 0, 0, 0, 1)}, http2.ErrCodeFrameSize},
		{"WINDOW_UPDATE on idle stream", []func(*testConn) error{raw(http2.FrameWindowUpdate, 0, 9, 0, 0, 0, 1)}, http2.ErrCodeProtocol},
		{"CONTINUATION alone", []func(*testConn) error{raw(http2.FrameContinuation, http2.FlagContinuationEndHeaders, 1, 0x82)}, http2.ErrCodeProtocol},
		{"frame over MAX_FRAME_SIZE", []func(*testConn) error{raw(http2.FrameData, 0, 1, make([]byte, 1<<14+1)...)}, http2.ErrCodeFrameSize},
		{"padding too long", []func(*testConn) error{headers(1), raw(http2.FrameData, http2.FlagDataPadded, 1, 5, 'x')}, http2.ErrCodeProtocol},
		{"PRIORITY on stream 0", []func(*testConn) error{raw(http2.FramePriority, 0, 0, 0, 0, 0, 1, 16)}, http2.ErrCodeProtocol},
		{"PRIORITY_UPDATE on a stream", []func(*testConn) error{raw(0x10, 0, 1, 0, 0, 0, 1, 'u', '=', '1')}, http2.ErrCodeProtocol},
		{"PRIORITY_UPDATE for even stream", []func(*testConn) error{raw(0x10, 0, 0, 0, 0, 0, 2, 'u', '=', '1')}, http2.ErrCodeProtocol},
		{"GOAWAY on a stream", []func(*testConn) error{raw(http2.FrameGoAway, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0)}, http2.ErrCodeProtocol},
		{"HPACK garbage", []func(*testConn) error{raw(http2.FrameHeaders, http2.FlagHeadersEndHeaders, 1, 0xff, 0xff, 0xff, 0xff)}, http2.ErrCodeCompression},
		{"HEADERS interrupted", []func(*testConn) error{raw(http2.FrameHeaders, 0, 1, 0x82), raw(http2.FramePing, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)}, http2.ErrCodeProtocol},
	} {
		t.Run(tt.name, func(t *testing.T) {
			tc := startConn(t, okServe, nil)
			for _, f := range tt.frames {
				tc.write(func() error { return f(tc) })
			}
			tc.expectGoAway(tt.want)
		})
	}
}

func TestStreamErrors(t *testing.T) {
	open := func(tc *testConn) error {
		return tc.fr.WriteHeaders(http2.HeadersFrameParam{StreamID: 1, EndHeaders: true,
			BlockFragment: tc.headerBlock(":method", "POST", ":scheme", "https", ":path", "/")})
	}
	for _, tt := range []struct {
		name   string
		frames []func(*testConn) error
		want   http2.ErrCode
	}{
		{"PRIORITY on itself", []func(*testConn) error{raw(http2.FramePriority, 0, 1, 0, 0, 0, 1, 16)}, http2.ErrCodeProtocol},
		{"PRIORITY bad length", []func(*testConn) error{raw(http2.FramePriority, 0, 1, 0, 0, 0, 3)}, http2.ErrCodeFrameSize},
		{"HEADERS depending on itself", []func(*testConn) error{func(tc *testConn) error {
			return tc.fr.WriteHeaders(http2.HeadersFrameParam{StreamID: 1, EndHeaders: true, EndStream: true,
				BlockFragment: tc.headerBlock(":method", "GET", ":scheme", "https", ":path", "/"),
				Priority:      http2.PriorityParam{StreamDep: 1, Weight: 16}})
		}}, http2.ErrCodeProtocol},
		{"WINDOW_UPDATE zero on stream", []func(*testConn) error{open, raw(http2.FrameWindowUpdate, 0, 1, 0, 0, 0, 0)}, http2.ErrCodeProtocol},
		{"stream window overflow", []func(*testConn) error{open, raw(http2.FrameWindowUpdate, 0, 1, 0x7f, 0xff, 0xff, 0xff)}, http2.ErrCodeFlowControl},
		{"trailers without END_STREAM", []func(*testConn) error{open, func(tc *testConn) error {
			return tc.fr.WriteHeaders(http2.HeadersFrameParam{StreamID: 1, EndHeaders: true, BlockFragment: tc.headerBlock("x-t", "v")})
		}}, http2.ErrCodeProtocol},
		{"pseudo-header in trailers", []func(*testConn) error{open, func(tc *testConn) error {
			return tc.fr.WriteHeaders(http2.HeadersFrameParam{StreamID: 1, EndHeaders: true, EndStream: true,
				BlockFragment: tc.headerBlock(":path", "/")})
		}}, http2.ErrCodeProtocol},
		{"DATA after END_STREAM", []func(*testConn) error{open, raw(http2.FrameData, http2.FlagDataEndStream, 1, 'a'),
			raw(http2.FrameData, 0, 1, 'b')}, http2.ErrCodeStreamClosed},
		{"DATA after client reset", []func(*testConn) error{open, raw(http2.FrameRSTStream, 0, 1, 0, 0, 0, 8),
			raw(http2.FrameData, 0, 1, 'b')}, http2.ErrCodeStreamClosed},
	} {
		t.Run(tt.name, func(t *testing.T) {
			block := make(chan struct{})
			defer close(block)
			tc := startConn(t, func(x *fakeExchange, w exchange.ResponseSink) error {
				<-block
				return okServe(x, w)
			}, nil)
			for _, f := range tt.frames {
				tc.write(func() error { return f(tc) })
			}
			tc.expectRST(1, tt.want)
		})
	}
}

func TestSendFlowControl(t *testing.T) {
	payload := bytes.Repeat([]byte("x"), 100)
	tc := startConn(t, func(x *fakeExchange, w exchange.ResponseSink) error {
		if err := w.WriteHeader(200, nil, -1); err != nil {
			return err
		}
		if _, err := w.Write(payload); err != nil {
			return err
		}
		return w.Finish()
	}, nil)
	// Frames are processed in order, so the window applies to the stream opened next.
	tc.write(func() error { return tc.fr.WriteSettings(http2.Setting{ID: http2.SettingInitialWindowSize, Val: 10}) })
	tc.get(1, true)
	got := 0
	for got < 10 {
		switch f := tc.read().(type) {
		case *http2.DataFrame:
			got += len(f.Data())
		}
	}
	if got != 10 {
		t.Fatalf("sent %d bytes past a 10-byte window", got)
	}
	// Raising the initial window through SETTINGS applies to open streams too.
	tc.write(func() error { return tc.fr.WriteSettings(http2.Setting{ID: http2.SettingInitialWindowSize, Val: 60}) })
	tc.write(func() error { return tc.fr.WriteWindowUpdate(1, 40) })
	for got < 100 {
		if f, ok := tc.read().(*http2.DataFrame); ok {
			got += len(f.Data())
		}
	}
	if f := tc.read(); f.Header().Type != http2.FrameData || !f.(*http2.DataFrame).StreamEnded() {
		t.Fatalf("missing END_STREAM: %v", f)
	}
}

func TestStreamingSinkFeatures(t *testing.T) {
	tc := startConn(t, func(x *fakeExchange, w exchange.ResponseSink) error {
		is := w.(exchange.InformationalSink)
		if err := is.WriteInformational(103, fields{{"link", "</a.css>; rel=preload"}}); err != nil {
			return err
		}
		if err := w.WriteHeader(200, fields{{"connection", "close"}, {"x-a", "1"}}, -1); err != nil {
			return err
		}
		if _, err := w.Write([]byte("part")); err != nil {
			return err
		}
		if err := w.Flush(); err != nil {
			return err
		}
		return w.(exchange.TrailerSink).FinishTrailer(fields{{"content-length", "9"}, {"grpc-status", "0"}})
	}, nil)
	tc.get(1, true)
	if h := tc.read().(*http2.MetaHeadersFrame); h.PseudoValue("status") != "103" || h.Fields[1].Value != "</a.css>; rel=preload" {
		t.Fatalf("103: %v", h.Fields)
	}
	h := tc.read().(*http2.MetaHeadersFrame)
	for _, f := range h.Fields {
		if f.Name == "connection" {
			t.Fatal("connection-specific field sent over HTTP/2")
		}
	}
	if d := tc.read().(*http2.DataFrame); string(d.Data()) != "part" {
		t.Fatalf("data %q", d.Data())
	}
	tr := tc.read().(*http2.MetaHeadersFrame)
	if !tr.StreamEnded() || len(tr.Fields) != 1 || tr.Fields[0].Name != "grpc-status" {
		t.Fatalf("trailers %v", tr.Fields)
	}
}

func TestPaddedFramesAndPriorityUpdate(t *testing.T) {
	var gotBody []byte
	done := make(chan struct{})
	tc := startConn(t, func(x *fakeExchange, w exchange.ResponseSink) error {
		gotBody, _ = io.ReadAll(x.body)
		close(done)
		return okServe(x, w)
	}, nil)
	block := tc.headerBlock(":method", "POST", ":scheme", "https", ":path", "/", "priority", "u=5")
	tc.write(func() error {
		return tc.fr.WriteHeaders(http2.HeadersFrameParam{StreamID: 1, EndHeaders: true, BlockFragment: block, PadLength: 3})
	})
	update := raw(0x10, 0, 0, append([]byte{0, 0, 0, 1}, "u=0, i"...)...)
	tc.write(func() error { return update(tc) })
	tc.write(func() error { return tc.fr.WriteDataPadded(1, true, []byte("abc"), []byte{0, 0}) })
	<-done
	if string(gotBody) != "abc" {
		t.Fatalf("body %q", gotBody)
	}
}

func TestReadTimeoutResetsStream(t *testing.T) {
	cfg := testConfig()
	cfg.ReadTimeout = 20 * time.Millisecond
	tc := startConn(t, okServe, cfg)
	tc.get(1, false) // the body never arrives
	tc.expectRST(1, http2.ErrCodeCancel)
}

func TestWriteTimeoutResetsStream(t *testing.T) {
	cfg := testConfig()
	cfg.WriteTimeout = 20 * time.Millisecond
	tc := startConn(t, func(x *fakeExchange, w exchange.ResponseSink) error {
		<-x.ctx.Done()
		return x.ctx.Err()
	}, cfg)
	tc.get(1, true)
	tc.expectRST(1, http2.ErrCodeCancel)
}

func TestIdleTimeout(t *testing.T) {
	cfg := testConfig()
	cfg.IdleTimeout = 30 * time.Millisecond
	tc := startConn(t, okServe, cfg)
	tc.expectGoAway(http2.ErrCodeNo)
}

func TestErrCodeString(t *testing.T) {
	if ErrCodeEnhanceYourCalm.String() != "ENHANCE_YOUR_CALM" || ErrCode(0x99).String() != "ERR_0x99" {
		t.Fatal("ErrCode.String")
	}
	if !strings.Contains(connError{ErrCodeProtocol, "x"}.Error(), "PROTOCOL_ERROR") ||
		!strings.Contains(streamError{3, ErrCodeCancel, "y"}.Error(), "stream 3") {
		t.Fatal("error strings")
	}
}
