package h3

import (
	"bufio"
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"math/big"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/erizeta/qilat/internal/exchange"
	"github.com/erizeta/qilat/internal/httpfields"
	"github.com/quic-go/qpack"
	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/quicvarint"
)

// fakeExchange is a minimal core that records the request and runs serve.
type fakeExchange struct {
	method, target, host string
	header               [][2]string
	body                 io.Reader
	contentLength        int64
	ctx                  context.Context
	cancel               context.CancelFunc
	serve                func(*fakeExchange, exchange.ResponseSink) error
}

func newFake(serve func(*fakeExchange, exchange.ResponseSink) error) *fakeExchange {
	ctx, cancel := context.WithCancel(context.Background())
	return &fakeExchange{serve: serve, ctx: ctx, cancel: cancel}
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
func (x *fakeExchange) Release()                                        { x.cancel() }

type fields [][2]string

func (f fields) Len() int                      { return len(f) }
func (f fields) At(i int) (name, value []byte) { return []byte(f[i][0]), []byte(f[i][1]) }

func okServe(x *fakeExchange, w exchange.ResponseSink) error {
	if x.body != nil {
		_, _ = io.Copy(io.Discard, x.body)
	}
	return w.(exchange.CompleteSink).WriteComplete(200, fields{{"content-type", "text/plain"}}, 2, []byte("ok"), nil)
}

func testCert(t testing.TB) tls.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(1), NotBefore: time.Now().Add(-time.Hour),
		NotAfter: time.Now().Add(time.Hour), DNSNames: []string{"localhost"}}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
}

func testConfig(serve func(*fakeExchange, exchange.ResponseSink) error) *Config {
	return &Config{
		Acquire:              func() exchange.Exchange { return newFake(serve) },
		MaxFieldSectionSize:  16 << 10,
		MaxRequestBodySize:   1 << 20,
		MaxConcurrentStreams: 10,
	}
}

// startServer serves HTTP/3 with cfg on loopback and returns a connected client.
func startServer(t *testing.T, cfg *Config) *quic.Conn {
	t.Helper()
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ln, err := Listen(pc, &tls.Config{Certificates: []tls.Certificate{testCert(t)}}, QUICConfig{MaxIncomingStreams: 100}, cfg)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			c, err := ln.Accept(context.Background())
			if err != nil {
				return
			}
			go c.Serve()
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := quic.DialAddr(ctx, pc.LocalAddr().String(),
		&tls.Config{InsecureSkipVerify: true, NextProtos: []string{"h3"}}, &quic.Config{}) //nolint:gosec // test certificate
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = conn.CloseWithError(0, "")
		_ = ln.Close()
		<-done
		_ = pc.Close()
	})
	return conn
}

func encodeFields(fs ...string) []byte {
	var b bytes.Buffer
	enc := qpack.NewEncoder(&b)
	for i := 0; i < len(fs); i += 2 {
		_ = enc.WriteField(qpack.HeaderField{Name: fs[i], Value: fs[i+1]})
	}
	return b.Bytes()
}

func frame(typ uint64, payload []byte) []byte {
	return append(appendFrameHeader(nil, typ, uint64(len(payload))), payload...)
}

var getFields = []string{":method", "GET", ":scheme", "https", ":path", "/", ":authority", "localhost"}

// request writes raw bytes on a new request stream and reads the whole response.
func request(t *testing.T, conn *quic.Conn, raw []byte) (status string, hdr [][2]string, body []byte, err error) {
	t.Helper()
	str, err := conn.OpenStreamSync(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	_ = str.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err := str.Write(raw); err != nil {
		return "", nil, nil, err
	}
	_ = str.Close()
	br := bufio.NewReader(str)
	for {
		typ, length, err := readFrameHeader(br)
		if errors.Is(err, io.EOF) {
			return status, hdr, body, nil
		}
		if err != nil {
			return status, hdr, body, err
		}
		p := make([]byte, length)
		if _, err := io.ReadFull(br, p); err != nil {
			return status, hdr, body, err
		}
		switch typ {
		case frameHeaders:
			next := qpack.NewDecoder().Decode(p)
			for {
				f, err := next()
				if err != nil {
					break
				}
				if f.Name == ":status" {
					if status == "" || strings.HasPrefix(status, "1") {
						status = f.Value
					}
					continue
				}
				hdr = append(hdr, [2]string{f.Name, f.Value})
			}
		case frameData:
			body = append(body, p...)
		}
	}
}

// streamErrorCode returns the HTTP/3 code a stream error carries, or 0.
func streamErrorCode(err error) quic.StreamErrorCode {
	var se *quic.StreamError
	if errors.As(err, &se) {
		return se.ErrorCode
	}
	return 0
}

// connErrorCode waits for the connection to close and returns the server's error code.
func connErrorCode(t *testing.T, conn *quic.Conn) quic.ApplicationErrorCode {
	t.Helper()
	select {
	case <-conn.Context().Done():
	case <-time.After(5 * time.Second):
		t.Fatal("connection not closed")
	}
	var ae *quic.ApplicationError
	if !errors.As(context.Cause(conn.Context()), &ae) {
		t.Fatalf("close cause %v", context.Cause(conn.Context()))
	}
	return ae.ErrorCode
}

func TestRequestResponse(t *testing.T) {
	seen := make(chan *fakeExchange, 1)
	conn := startServer(t, testConfig(func(x *fakeExchange, w exchange.ResponseSink) error {
		seen <- x
		return okServe(x, w)
	}))
	raw := frame(0x21, []byte("grease")) // unknown frame types before HEADERS are ignored
	raw = append(raw, frame(frameHeaders, encodeFields(append(getFields, "cookie", "a=1", "cookie", "b=2", "x-a", "1")...))...)
	status, hdr, body, err := request(t, conn, raw)
	if err != nil || status != "200" || string(body) != "ok" {
		t.Fatalf("status %s body %q err %v", status, body, err)
	}
	got := <-seen
	if got.method != "GET" || got.target != "/" || got.host != "localhost" {
		t.Fatalf("request %+v", got)
	}
	if v := lookup(got.header, "cookie"); v != "a=1; b=2" {
		t.Fatalf("cookie %q", v)
	}
	if lookup(hdr, "date") == "" || lookup(hdr, "content-type") != "text/plain" {
		t.Fatalf("response header %v", hdr)
	}
}

func lookup(h [][2]string, name string) string {
	for _, f := range h {
		if f[0] == name {
			return f[1]
		}
	}
	return ""
}

func TestRequestBodyAndTrailers(t *testing.T) {
	conn := startServer(t, testConfig(func(x *fakeExchange, w exchange.ResponseSink) error {
		b, err := io.ReadAll(x.body)
		if err != nil {
			return err
		}
		if err := w.WriteHeader(200, nil, -1); err != nil {
			return err
		}
		if _, err := w.Write(bytes.ToUpper(b)); err != nil {
			return err
		}
		return w.(exchange.TrailerSink).FinishTrailer(fields{{"x-sum", "3"}, {"content-length", "1"}})
	}))
	raw := frame(frameHeaders, encodeFields(":method", "POST", ":scheme", "https", ":path", "/", "content-length", "6"))
	raw = append(raw, frame(frameData, []byte("abc"))...)
	raw = append(raw, frame(0x21, nil)...)
	raw = append(raw, frame(frameData, []byte("def"))...)
	raw = append(raw, frame(frameHeaders, encodeFields("x-t", "v"))...)
	status, hdr, body, err := request(t, conn, raw)
	if err != nil || status != "200" || string(body) != "ABCDEF" || lookup(hdr, "x-sum") != "3" || lookup(hdr, "content-length") != "" {
		t.Fatalf("status %s body %q hdr %v err %v", status, body, hdr, err)
	}
}

func TestStreamErrors(t *testing.T) {
	for _, tt := range []struct {
		name string
		raw  []byte
		want quic.StreamErrorCode
	}{
		{"uppercase field", frame(frameHeaders, encodeFields(append(getFields, "X-Up", "v")...)), errMessage},
		{"connection field", frame(frameHeaders, encodeFields(append(getFields, "connection", "close")...)), errMessage},
		{"missing path", frame(frameHeaders, encodeFields(":method", "GET", ":scheme", "https")), errMessage},
		{"content-length mismatch", append(frame(frameHeaders, encodeFields(":method", "POST", ":scheme", "https",
			":path", "/", "content-length", "2")), frame(frameData, []byte("abc"))...), errMessage},
		{"no HEADERS", nil, errRequestIncomplete},
		{"malformed trailers", append(frame(frameHeaders, encodeFields(":method", "POST", ":scheme", "https", ":path", "/")),
			frame(frameHeaders, encodeFields(":path", "/"))...), errMessage},
	} {
		t.Run(tt.name, func(t *testing.T) {
			conn := startServer(t, testConfig(okServe))
			_, _, _, err := request(t, conn, tt.raw)
			if got := streamErrorCode(err); got != tt.want {
				t.Fatalf("stream error %v (%v), want %#x", got, err, uint64(tt.want))
			}
		})
	}
}

func TestRejectedRequests(t *testing.T) {
	cfg := testConfig(okServe)
	cfg.MaxFieldSectionSize = 1024
	cfg.MaxRequestBodySize = 10
	conn := startServer(t, cfg)
	for _, tt := range []struct {
		name string
		fs   []string
		want string
	}{
		{"header too large", append(getFields, "x-big", strings.Repeat("a", 1500)), "431"},
		{"encoded section too large", append(getFields, "x-big", strings.Repeat("a", 3000)), "431"},
		{"body too large", []string{":method", "POST", ":scheme", "https", ":path", "/", "content-length", "11"}, "413"},
	} {
		status, _, body, err := request(t, conn, frame(frameHeaders, encodeFields(tt.fs...)))
		if err != nil || status != tt.want || !strings.HasPrefix(string(body), tt.want) {
			t.Errorf("%s: status %s body %q err %v", tt.name, status, body, err)
		}
	}
}

func TestExpectContinue(t *testing.T) {
	conn := startServer(t, testConfig(okServe))
	str, err := conn.OpenStreamSync(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	_ = str.SetDeadline(time.Now().Add(5 * time.Second))
	_, _ = str.Write(frame(frameHeaders, encodeFields(":method", "POST", ":scheme", "https", ":path", "/", "expect", "100-continue")))
	br := bufio.NewReader(str)
	typ, length, err := readFrameHeader(br)
	if err != nil || typ != frameHeaders {
		t.Fatalf("frame %d: %v", typ, err)
	}
	p := make([]byte, length)
	_, _ = io.ReadFull(br, p)
	if f, _ := qpack.NewDecoder().Decode(p)(); f.Name != ":status" || f.Value != "100" {
		t.Fatalf("first field %v, want :status 100", f)
	}
	_, _ = str.Write(frame(frameData, []byte("x")))
	_ = str.Close()
}

func TestConnectionErrors(t *testing.T) {
	settings := func(pairs ...uint64) []byte {
		var p []byte
		for _, v := range pairs {
			p = quicvarint.Append(p, v)
		}
		return frame(frameSettings, p)
	}
	uni := func(typ uint64, rest ...[]byte) func(*quic.Conn) error {
		return func(conn *quic.Conn) error {
			s, err := conn.OpenUniStream()
			if err != nil {
				return err
			}
			b := quicvarint.Append(nil, typ)
			for _, r := range rest {
				b = append(b, r...)
			}
			_, err = s.Write(b)
			return err
		}
	}
	reqStream := func(raw []byte) func(*quic.Conn) error {
		return func(conn *quic.Conn) error {
			s, err := conn.OpenStream()
			if err != nil {
				return err
			}
			_, err = s.Write(raw)
			_ = s.Close()
			return err
		}
	}
	for _, tt := range []struct {
		name  string
		steps []func(*quic.Conn) error
		want  quic.ApplicationErrorCode
	}{
		{"missing SETTINGS", []func(*quic.Conn) error{uni(streamControl, frame(frameGoAway, []byte{0}))}, errMissingSettings},
		{"second control stream", []func(*quic.Conn) error{uni(streamControl, settings()), uni(streamControl, settings())}, errStreamCreation},
		{"HTTP/2 setting", []func(*quic.Conn) error{uni(streamControl, settings(0x4, 100))}, errSettings},
		{"duplicate setting", []func(*quic.Conn) error{uni(streamControl, settings(0x6, 1, 0x6, 2))}, errSettings},
		{"DATA on control stream", []func(*quic.Conn) error{uni(streamControl, settings(), frame(frameData, nil))}, errFrameUnexpected},
		{"HTTP/2 frame type", []func(*quic.Conn) error{uni(streamControl, settings(), frame(0x6, nil))}, errFrameUnexpected},
		{"push stream", []func(*quic.Conn) error{uni(streamPush)}, errStreamCreation},
		{"dynamic table insert", []func(*quic.Conn) error{uni(streamQPACKEncoder, []byte{0xc0 | 1, 0x01, 'x'})}, errQPACKEncoderStream},
		{"DATA before HEADERS", []func(*quic.Conn) error{reqStream(frame(frameData, []byte("x")))}, errFrameUnexpected},
		{"dynamic table reference", []func(*quic.Conn) error{reqStream(frame(frameHeaders, []byte{0x01, 0x00, 0x80}))}, errQPACKDecompression},
		{"control stream closed", []func(*quic.Conn) error{func(conn *quic.Conn) error {
			s, err := conn.OpenUniStream()
			if err != nil {
				return err
			}
			_, err = s.Write(append(quicvarint.Append(nil, streamControl), settings()...))
			_ = s.Close()
			return err
		}}, errClosedCriticalStream},
	} {
		t.Run(tt.name, func(t *testing.T) {
			conn := startServer(t, testConfig(okServe))
			for _, step := range tt.steps {
				if err := step(conn); err != nil {
					t.Fatal(err)
				}
			}
			if got := connErrorCode(t, conn); got != tt.want {
				t.Fatalf("closed with %#x, want %#x", uint64(got), uint64(tt.want))
			}
		})
	}
}

func TestUnknownUniStreamIsIgnored(t *testing.T) {
	conn := startServer(t, testConfig(okServe))
	s, err := conn.OpenUniStream()
	if err != nil {
		t.Fatal(err)
	}
	_, _ = s.Write(quicvarint.Append(nil, 0x21))
	if status, _, _, err := request(t, conn, frame(frameHeaders, encodeFields(getFields...))); err != nil || status != "200" {
		t.Fatalf("status %s err %v", status, err)
	}
}

func TestHandlersBoundedWhenClientResets(t *testing.T) {
	release := make(chan struct{})
	var running, peak atomic.Int64
	cfg := testConfig(func(x *fakeExchange, w exchange.ResponseSink) error {
		n := running.Add(1)
		for p := peak.Load(); n > p && !peak.CompareAndSwap(p, n); {
			p = peak.Load()
		}
		<-release // handlers ignore cancellation, the worst case for rapid resets
		running.Add(-1)
		return okServe(x, w)
	})
	cfg.MaxConcurrentStreams = 5
	conn := startServer(t, cfg)
	raw := frame(frameHeaders, encodeFields(getFields...))
	for range 200 {
		s, err := conn.OpenStreamSync(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		_, _ = s.Write(raw)
		s.CancelWrite(errRequestCancelled)
		s.CancelRead(errRequestCancelled)
	}
	time.Sleep(200 * time.Millisecond)
	if p := peak.Load(); p > 5 {
		t.Fatalf("%d handlers ran at once, limit is 5", p)
	}
	close(release)
}

func TestGoAwayRejectsNewStreams(t *testing.T) {
	proceed := make(chan struct{})
	cfg := testConfig(func(x *fakeExchange, w exchange.ResponseSink) error {
		<-proceed
		return okServe(x, w)
	})
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = pc.Close() }()
	ln, err := Listen(pc, &tls.Config{Certificates: []tls.Certificate{testCert(t)}}, QUICConfig{}, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	srvConn := make(chan *Conn, 1)
	go func() {
		c, err := ln.Accept(context.Background())
		if err == nil {
			srvConn <- c
			c.Serve()
		}
	}()
	conn, err := quic.DialAddr(context.Background(), pc.LocalAddr().String(),
		&tls.Config{InsecureSkipVerify: true, NextProtos: []string{"h3"}}, &quic.Config{}) //nolint:gosec // test certificate
	if err != nil {
		t.Fatal(err)
	}
	server := <-srvConn
	ctrl, err := conn.AcceptUniStream(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	inflight := make(chan string, 1)
	go func() {
		status, _, _, _ := request(t, conn, frame(frameHeaders, encodeFields(getFields...)))
		inflight <- status
	}()
	for {
		server.mu.Lock()
		active := server.active
		server.mu.Unlock()
		if active == 1 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if server.CloseIfIdle() {
		t.Fatal("closed with an active request")
	}
	br := bufio.NewReader(ctrl)
	if typ, _ := quicvarint.Read(br); typ != streamControl {
		t.Fatalf("stream type %d", typ)
	}
	for {
		typ, length, err := readFrameHeader(br)
		if err != nil {
			t.Fatal(err)
		}
		p := make([]byte, length)
		_, _ = io.ReadFull(br, p)
		if typ == frameGoAway {
			if id, _, _ := quicvarint.Parse(p); id != 4 {
				t.Fatalf("GOAWAY id %d, want 4", id)
			}
			break
		}
	}
	// A request the client opens after GOAWAY is rejected without running.
	_, _, _, err = request(t, conn, frame(frameHeaders, encodeFields(getFields...)))
	if got := streamErrorCode(err); got != errRequestRejected {
		t.Fatalf("stream after GOAWAY: %v", err)
	}
	close(proceed)
	if got := <-inflight; got != "200" {
		t.Fatalf("in-flight request: %s", got)
	}
	if code := connErrorCode(t, conn); code != errNoError {
		t.Fatalf("closed with %#x", uint64(code))
	}
}

func TestQPACKAgainstReference(t *testing.T) {
	cases := [][]string{
		{":status", "200", "content-type", "text/html; charset=utf-8", "x-custom", strings.Repeat("v", 300), "x-empty", ""},
		{":status", "418", "vary", "accept-encoding", "set-cookie", "a=b"},
		{":method", "GET", ":path", "/index.html", ":authority", "example.com", "user-agent", "test/1.0"},
	}
	for _, fs := range cases {
		// Our encoding decoded by quic-go/qpack.
		b := appendSectionPrefix(nil)
		for i := 0; i < len(fs); i += 2 {
			b = appendField(b, []byte(fs[i]), []byte(fs[i+1]))
		}
		next := qpack.NewDecoder().Decode(b)
		for i := 0; i < len(fs); i += 2 {
			f, err := next()
			if err != nil || f.Name != fs[i] || f.Value != fs[i+1] {
				t.Fatalf("reference decoded %v %v, want %s: %s", f, err, fs[i], fs[i+1])
			}
		}
		// quic-go/qpack's encoding decoded by ours.
		x := newFake(nil)
		var req httpfields.Request
		req.Reset(x, false, 1<<20)
		var d decoder
		if err := d.decode(encodeFields(fs...), &req); err != nil {
			t.Fatal(err)
		}
	}
}

func FuzzQPACKDecode(f *testing.F) {
	f.Add(encodeFields(getFields...))
	f.Add(encodeFields(":status", "200", "x-a", strings.Repeat("b", 200)))
	f.Add([]byte{0, 0, 0x5f, 0x1d, 0x85, 0x41, 0x6c, 0xee, 0x5b, 0x3f})
	f.Fuzz(func(t *testing.T, p []byte) {
		// Ours must accept exactly the static-only sections quic-go/qpack accepts, with the same fields.
		var want []string
		next := qpack.NewDecoder().Decode(p)
		refErr := error(nil)
		for {
			hf, err := next()
			if err != nil {
				if !errors.Is(err, io.EOF) {
					refErr = err
				}
				break
			}
			want = append(want, hf.Name+"\x00"+hf.Value)
		}
		var got []string
		var d decoder
		var c collector
		err := d.decode(p, &c)
		got = c.fields
		if (err == nil) != (refErr == nil) {
			t.Fatalf("ours: %v, reference: %v", err, refErr)
		}
		if err == nil && strings.Join(got, "\x01") != strings.Join(want, "\x01") {
			t.Fatalf("ours %q, reference %q", got, want)
		}
	})
}

type collector struct{ fields []string }

func (c *collector) Field(name, value []byte) {
	c.fields = append(c.fields, string(name)+"\x00"+string(value))
}

// FuzzRequestStream feeds arbitrary bytes to the request stream parser: frames, QPACK, body, and trailers.
func FuzzRequestStream(f *testing.F) {
	f.Add(frame(frameHeaders, encodeFields(getFields...)))
	f.Add(append(frame(frameHeaders, encodeFields(":method", "POST", ":scheme", "https", ":path", "/", "content-length", "3")),
		append(frame(frameData, []byte("abc")), frame(frameHeaders, encodeFields("x", "y"))...)...))
	f.Fuzz(func(t *testing.T, data []byte) {
		c := &Conn{cfg: testConfig(okServe)}
		s := &stream{c: c, br: bufio.NewReaderSize(bytes.NewReader(data), 64)}
		s.body = body{s: s, declared: -1, limit: 1 << 20}
		length, err := s.firstHeaders()
		if err != nil || length > 1<<16 {
			return
		}
		x := newFake(nil)
		s.x = x
		s.req.Reset(x, false, c.cfg.MaxFieldSectionSize)
		if s.readSection(length) != nil || s.req.Finish(3) != "" {
			return
		}
		s.body.declared = s.req.ContentLength
		_, _ = io.Copy(io.Discard, &s.body)
	})
}

// eofWithData returns the last bytes of the stream together with io.EOF, as quic-go may.
type eofWithData struct{ r *bytes.Reader }

func (e eofWithData) Read(p []byte) (int, error) {
	n, err := e.r.Read(p)
	if err == nil && e.r.Len() == 0 {
		err = io.EOF
	}
	return n, err
}

func TestBodyEndsWithDataAndEOF(t *testing.T) {
	raw := frame(frameData, bytes.Repeat([]byte("x"), 10000))
	c := &Conn{cfg: testConfig(okServe)}
	s := &stream{c: c, br: bufio.NewReaderSize(eofWithData{bytes.NewReader(raw)}, 16)}
	s.body = body{s: s, declared: 10000, limit: -1}
	s.x = newFake(nil)
	n, err := io.CopyBuffer(io.Discard, &s.body, make([]byte, 32<<10))
	if err != nil || n != 10000 {
		t.Fatalf("read %d bytes: %v", n, err)
	}
}
