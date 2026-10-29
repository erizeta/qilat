package qilat

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	"github.com/erizeta/qilat/internal/exchange"
)

// fakeSink records what the core sends, standing in for a protocol.
type fakeSink struct {
	status      int
	header      []string
	cl          int64
	body        bytes.Buffer
	headerCalls int
	flushes     int
	finishes    int
	failHeader  error
	failWrite   error
	failFlush   error
	failFinish  error
}

func (s *fakeSink) WriteHeader(status int, h exchange.HeaderList, contentLength int64) error {
	s.headerCalls++
	s.status, s.cl = status, contentLength
	s.header = s.header[:0]
	for i := range h.Len() {
		name, value := h.At(i)
		s.header = append(s.header, string(name)+"="+string(value))
	}
	return s.failHeader
}

func (s *fakeSink) Write(p []byte) (int, error) {
	if s.failWrite != nil {
		return 0, s.failWrite
	}
	return s.body.Write(p)
}

func (s *fakeSink) Flush() error  { s.flushes++; return s.failFlush }
func (s *fakeSink) Finish() error { s.finishes++; return s.failFinish }

// request describes what the fake protocol puts into an exchange.
type request struct {
	method, target, host string
	headers              [][2]string
	body                 string
	major, minor         int
}

func (rq request) fill(t *testing.T, x exchange.Exchange) {
	t.Helper()
	method, target := rq.method, rq.target
	if method == "" {
		method = "GET"
	}
	if target == "" {
		target = "/"
	}
	if err := x.SetMethod([]byte(method)); err != nil {
		t.Fatal(err)
	}
	if err := x.SetTarget([]byte(target)); err != nil {
		t.Fatal(err)
	}
	if err := x.SetHost([]byte(rq.host)); err != nil {
		t.Fatal(err)
	}
	if rq.major != 0 {
		x.SetProto(rq.major, rq.minor)
	}
	for _, h := range rq.headers {
		if err := x.AddHeader([]byte(h[0]), []byte(h[1])); err != nil {
			t.Fatal(err)
		}
	}
	if rq.body != "" {
		x.SetBody(bytes.NewReader([]byte(rq.body)), int64(len(rq.body)))
	}
}

// serve runs one request through a pooled exchange and returns what the sink received.
func serve(t *testing.T, rq request, h HandlerFunc) (*fakeSink, error) {
	t.Helper()
	sink := &fakeSink{}
	x := acquireExchange(&exchangeConfig{handler: h})
	rq.fill(t, x)
	err := x.Serve(sink)
	x.Release()
	return sink, err
}

func TestServeSmallResponse(t *testing.T) {
	sink, err := serve(t, request{}, func(w ResponseWriter, r *Request) {
		w.Header().Set("Content-Type", "text/plain")
		_, _ = w.WriteString("hello")
	})
	if err != nil {
		t.Fatal(err)
	}
	if sink.status != 200 || sink.cl != 5 || sink.body.String() != "hello" || sink.finishes != 1 || sink.headerCalls != 1 {
		t.Fatalf("status %d cl %d body %q finishes %d headerCalls %d", sink.status, sink.cl, sink.body.String(), sink.finishes, sink.headerCalls)
	}
	if want := []string{"content-type=text/plain", "content-length=5"}; !equalStrings(sink.header, want) {
		t.Fatalf("header = %q, want %q", sink.header, want)
	}
}

func TestServeEmptyResponse(t *testing.T) {
	sink, err := serve(t, request{}, func(ResponseWriter, *Request) {})
	if err != nil {
		t.Fatal(err)
	}
	if sink.status != 200 || sink.cl != 0 || !equalStrings(sink.header, []string{"content-length=0"}) {
		t.Fatalf("status %d cl %d header %q", sink.status, sink.cl, sink.header)
	}
}

func TestServeRequestAccessors(t *testing.T) {
	rq := request{
		method: "POST", target: "/items/a%20b?id=7", host: "example.com", major: 2,
		headers: [][2]string{{"Content-Type", "application/json"}, {"X-Multi", "1"}, {"x-multi", "2"}},
		body:    `{"k":1}`,
	}
	_, err := serve(t, rq, func(w ResponseWriter, r *Request) {
		body, err := io.ReadAll(r.Body)
		switch {
		case err != nil:
			t.Errorf("read body: %v", err)
		case r.Method() != "POST":
			t.Errorf("Method = %q", r.Method())
		case string(r.Path()) != "/items/a b" || string(r.URI().RawPath()) != "/items/a%20b":
			t.Errorf("Path = %q", r.Path())
		case r.URI().Query().Get("id") != "7":
			t.Errorf("query id = %q", r.URI().Query().Get("id"))
		case string(r.Host()) != "example.com":
			t.Errorf("Host = %q", r.Host())
		case r.Proto() != "HTTP/2.0":
			t.Errorf("Proto = %q", r.Proto())
		case r.Header.Get("content-type") != "application/json" || len(r.Header.PeekAll("X-Multi")) != 2:
			t.Errorf("header = %v", fields(r.Header))
		case r.ContentLength() != 7 || string(body) != `{"k":1}`:
			t.Errorf("ContentLength %d body %q", r.ContentLength(), body)
		case r.RemoteAddr() != nil || r.TLS() != nil:
			t.Errorf("RemoteAddr %v TLS %v", r.RemoteAddr(), r.TLS())
		}
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestServeDefaultsAndNoLeakBetweenRequests(t *testing.T) {
	first := request{target: "/first?a=1", host: "one", headers: [][2]string{{"X-First", "1"}}, body: "data"}
	if _, err := serve(t, first, func(w ResponseWriter, r *Request) { w.Header().Set("X-Resp", "1") }); err != nil {
		t.Fatal(err)
	}
	sink, err := serve(t, request{target: "/second"}, func(w ResponseWriter, r *Request) {
		n, err := r.Body.Read(make([]byte, 1))
		if n != 0 || err != io.EOF {
			t.Errorf("empty body Read = %d, %v", n, err)
		}
		if r.Header.Len() != 0 || len(r.Host()) != 0 || r.URI().Query().Len() != 0 {
			t.Errorf("state leaked: header %v host %q query %d", fields(r.Header), r.Host(), r.URI().Query().Len())
		}
		if r.ContentLength() != -1 || r.Proto() != "HTTP/1.1" {
			t.Errorf("ContentLength %d Proto %q", r.ContentLength(), r.Proto())
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	if !equalStrings(sink.header, []string{"content-length=0"}) {
		t.Fatalf("response header leaked: %q", sink.header)
	}
}

func TestSetBodyNil(t *testing.T) {
	x := acquireExchange(&exchangeConfig{handler: HandlerFunc(func(ResponseWriter, *Request) {})})
	defer x.Release()
	x.SetBody(nil, 0)
	if _, ok := x.req.Body.(noBody); !ok {
		t.Fatalf("Body = %T, want noBody", x.req.Body)
	}
}

func TestExchangeRejectsInvalidInput(t *testing.T) {
	x := acquireExchange(&exchangeConfig{handler: HandlerFunc(func(ResponseWriter, *Request) {})})
	defer x.Release()
	for _, m := range []string{"", "GE T", "G\x00", "GET\r\n"} {
		if err := x.SetMethod([]byte(m)); err == nil {
			t.Errorf("SetMethod(%q) accepted", m)
		}
	}
	for _, h := range []string{"a b", "a/b", "a?b", "a#b", "u@h", "h\x7f", "h\r"} {
		if err := x.SetHost([]byte(h)); err == nil {
			t.Errorf("SetHost(%q) accepted", h)
		}
	}
	if err := x.SetTarget([]byte("/a b")); err == nil {
		t.Error("SetTarget accepted a space")
	}
	if err := x.AddHeader([]byte("x"), []byte("a\nb")); err == nil {
		t.Error("AddHeader accepted LF")
	}
}

func TestMethodInterning(t *testing.T) {
	for _, m := range standardMethods {
		if got := internMethod([]byte(m)); got != m {
			t.Errorf("internMethod(%q) = %q", m, got)
		}
	}
	custom := []byte("PROPFIND")
	got := internMethod(custom)
	custom[0] = 'X'
	if got != "PROPFIND" {
		t.Fatalf("custom method aliases input: %q", got)
	}
}

func TestSetProto(t *testing.T) {
	var r Request
	for _, tt := range []struct {
		major, minor int
		want         string
	}{{1, 1, "HTTP/1.1"}, {1, 0, "HTTP/1.0"}, {2, 0, "HTTP/2.0"}, {3, 0, "HTTP/3.0"}, {0, 9, "HTTP/0.9"}} {
		r.setProto(tt.major, tt.minor)
		if r.proto != tt.want {
			t.Errorf("setProto(%d, %d) = %q, want %q", tt.major, tt.minor, r.proto, tt.want)
		}
	}
}

func TestClone(t *testing.T) {
	type key struct{}
	var clone *Request
	_, err := serve(t, request{target: "/orig?q=1", host: "h", headers: [][2]string{{"X-A", "1"}}, body: "b"}, func(w ResponseWriter, r *Request) {
		clone = r.Clone(context.WithValue(context.Background(), key{}, "v"))
	})
	if err != nil {
		t.Fatal(err)
	}
	// Reuse the pooled exchange with a different request; the clone must be unaffected.
	if _, err := serve(t, request{target: "/other?q=2", host: "other", headers: [][2]string{{"X-A", "2"}}}, func(ResponseWriter, *Request) {}); err != nil {
		t.Fatal(err)
	}
	if string(clone.Path()) != "/orig" || clone.URI().Query().Get("q") != "1" || string(clone.Host()) != "h" ||
		clone.Header.Get("x-a") != "1" || clone.ContentLength() != 1 || clone.Method() != "GET" {
		t.Fatalf("clone changed: path %q host %q x-a %q", clone.Path(), clone.Host(), clone.Header.Get("x-a"))
	}
	if clone.Context().Value(key{}) != "v" {
		t.Fatal("clone context not set")
	}
	if n, err := clone.Body.Read(make([]byte, 1)); n != 0 || err != io.EOF {
		t.Fatalf("clone body Read = %d, %v", n, err)
	}
}

func TestWithContext(t *testing.T) {
	type key struct{}
	_, err := serve(t, request{headers: [][2]string{{"X-A", "1"}}}, func(w ResponseWriter, r *Request) {
		ctx := context.WithValue(r.Context(), key{}, "v")
		r2 := r.WithContext(ctx)
		if r2.Context() != ctx || r2.Context().Value(key{}) != "v" {
			t.Error("WithContext did not set context")
		}
		r2.Header.Set("X-A", "2")
		if r.Header.Get("x-a") != "2" {
			t.Error("WithContext copy does not share the header")
		}
		if r.Context() == ctx {
			t.Error("WithContext changed the original request")
		}
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestNilContextPanics(t *testing.T) {
	_, _ = serve(t, request{}, func(w ResponseWriter, r *Request) {
		for name, f := range map[string]func(){
			"WithContext": func() { r.WithContext(nil) }, //nolint:staticcheck // nil is the case under test
			"Clone":       func() { r.Clone(nil) },       //nolint:staticcheck // nil is the case under test
		} {
			func() {
				defer func() {
					if recover() == nil {
						t.Errorf("%s(nil) did not panic", name)
					}
				}()
				f()
			}()
		}
	})
}

func TestPoisonAfterRelease(t *testing.T) {
	var req *Request
	var resp ResponseWriter
	cfg := &exchangeConfig{handler: HandlerFunc(func(w ResponseWriter, r *Request) { req, resp = r, w }), poison: true}
	x := acquireExchange(cfg)
	request{}.fill(t, x)
	if err := x.Serve(&fakeSink{}); err != nil {
		t.Fatal(err)
	}
	x.Release()
	for name, f := range map[string]func(){
		"Method":         func() { req.Method() },
		"URI":            func() { req.URI() },
		"Path":           func() { req.Path() },
		"Host":           func() { req.Host() },
		"ContentLength":  func() { req.ContentLength() },
		"Proto":          func() { req.Proto() },
		"RemoteAddr":     func() { req.RemoteAddr() },
		"TLS":            func() { req.TLS() },
		"Context":        func() { req.Context() },
		"WithContext":    func() { req.WithContext(context.Background()) },
		"Clone":          func() { req.Clone(context.Background()) },
		"Header.Peek":    func() { req.Header.Peek("x") },
		"Header.Len":     func() { req.Header.Len() },
		"Header.All":     func() { req.Header.All() },
		"Header.Reset":   func() { req.Header.Reset() },
		"Header.CopyTo":  func() { req.Header.CopyTo(new(Header)) },
		"w.Header":       func() { resp.Header() },
		"w.WriteHeader":  func() { resp.WriteHeader(200) },
		"w.Write":        func() { _, _ = resp.Write(nil) },
		"w.Flush":        func() { _ = resp.Flush() },
		"w.WriteString":  func() { _, _ = resp.WriteString("") },
		"w.Header().Set": func() { resp.(*response).header.Set("a", "b") },
	} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("%s after Release did not panic", name)
				}
			}()
			f()
		}()
	}
}

func TestServeAllocs(t *testing.T) {
	if raceEnabled {
		t.Skip("allocation counts are not meaningful with the race detector")
	}
	body := []byte(`{"ok":true}`)
	run := func(h HandlerFunc) float64 {
		cfg := &exchangeConfig{handler: h}
		sink := discardSink{}
		method, target, host := []byte("GET"), []byte("/api/items?id=1"), []byte("example.com")
		name, value := []byte("Accept"), []byte("application/json")
		once := func() {
			x := acquireExchange(cfg)
			_ = x.SetMethod(method)
			_ = x.SetTarget(target)
			_ = x.SetHost(host)
			_ = x.AddHeader(name, value)
			_ = x.Serve(sink)
			x.Release()
		}
		once()
		return testing.AllocsPerRun(200, once)
	}
	plain := run(func(w ResponseWriter, r *Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = r.URI().Query().Peek("id")
		_, _ = w.Write(body)
	})
	if plain != 0 {
		t.Errorf("request without Context(): allocs = %v, want 0", plain)
	}
	withCtx := run(func(w ResponseWriter, r *Request) {
		_ = r.Context().Err()
		_, _ = w.Write(body)
	})
	if withCtx > 1 {
		t.Errorf("request calling Context(): allocs = %v, want at most 1", withCtx)
	}
}

func TestServeReturnsFinishError(t *testing.T) {
	boom := errors.New("boom")
	sink := &fakeSink{failFinish: boom}
	x := acquireExchange(&exchangeConfig{handler: HandlerFunc(func(ResponseWriter, *Request) {})})
	request{}.fill(t, x)
	if err := x.Serve(sink); !errors.Is(err, boom) {
		t.Fatalf("Serve error = %v, want boom", err)
	}
	x.Release()
}

// discardSink accepts a response without recording it, so allocation tests measure only the core.
type discardSink struct{}

func (discardSink) WriteHeader(int, exchange.HeaderList, int64) error { return nil }
func (discardSink) Write(p []byte) (int, error)                       { return len(p), nil }
func (discardSink) Flush() error                                      { return nil }
func (discardSink) Finish() error                                     { return nil }

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
