package qilat

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/quic-go/quic-go/http3"
)

type protoClient struct {
	name, base, proto string
	client            *http.Client
}

// startAllProtocols serves h over HTTP/1.1 and HTTP/2 (TLS) and HTTP/3, and returns a client for each.
func startAllProtocols(t *testing.T, h Handler) []protoClient {
	t.Helper()
	checkGoroutines(t)
	cert, err := GenerateDevCert()
	if err != nil {
		t.Fatal(err)
	}
	srv := &Server{
		Handler:   h,
		TLSConfig: &tls.Config{Certificates: []tls.Certificate{cert}},
		ErrorLog:  slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	ln := listenLocal(t)
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); _ = srv.ServeTLS(ln, "", "") }()
	go func() { defer wg.Done(); _ = srv.ServeQUIC(pc) }()
	insecure := &tls.Config{InsecureSkipVerify: true} //nolint:gosec // self-signed test certificate
	h1 := &http.Transport{TLSClientConfig: insecure.Clone()}
	h2 := &http.Transport{TLSClientConfig: insecure.Clone(), ForceAttemptHTTP2: true}
	h3 := &http3.Transport{TLSClientConfig: insecure.Clone()}
	t.Cleanup(func() {
		h1.CloseIdleConnections()
		h2.CloseIdleConnections()
		_ = h3.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
		wg.Wait()
	})
	client := func(rt http.RoundTripper) *http.Client { return &http.Client{Transport: rt, Timeout: 10 * time.Second} }
	return []protoClient{
		{"h1", "https://" + ln.Addr().String(), "HTTP/1.1", client(h1)},
		{"h2", "https://" + ln.Addr().String(), "HTTP/2.0", client(h2)},
		{"h3", "https://" + pc.LocalAddr().String(), "HTTP/3.0", client(h3)},
	}
}

func TestFromStdRequest(t *testing.T) {
	type seen struct {
		method, uri, path, query, host, proto, header, body, pattern, id string
		major                                                            int
		cl                                                               int64
		tls, remote, hijacker                                            bool
	}
	got := make(chan seen, 1)
	mux := NewServeMux()
	mux.Handle("POST /items/{id}", FromStd(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
		}
		_, hj := w.(http.Hijacker)
		got <- seen{
			method: r.Method, uri: r.RequestURI, path: r.URL.Path, query: r.URL.Query().Get("q"), host: r.Host,
			proto: r.Proto, major: r.ProtoMajor, header: strings.Join(r.Header["X-Multi"], ",") + "|" +
				r.Header.Get("Content-Type"), body: string(body), cl: r.ContentLength, pattern: r.Pattern,
			id: r.PathValue("id"), tls: r.TLS != nil, remote: r.RemoteAddr != "", hijacker: hj,
		}
	})))
	for _, c := range startAllProtocols(t, mux) {
		req, _ := http.NewRequest("POST", c.base+"/items/a%20b?q=x+y", strings.NewReader("payload"))
		req.Header.Add("X-Multi", "1")
		req.Header.Add("X-Multi", "2")
		req.Header.Set("Content-Type", "text/plain")
		resp, err := c.client.Do(req) //nolint:bodyclose // readAll closes the body
		readAll(t, resp, err)
		s := <-got
		want := seen{
			method: "POST", uri: "/items/a%20b?q=x+y", path: "/items/a b", query: "x y", host: req.URL.Host,
			proto: c.proto, major: int(c.proto[5] - '0'), header: "1,2|text/plain", body: "payload", cl: 7,
			pattern: "POST /items/{id}", id: "a b", tls: true, remote: true, hijacker: c.name == "h1",
		}
		if s != want {
			t.Errorf("%s:\n got %+v\nwant %+v", c.name, s, want)
		}
	}
}

func TestFromStdResponse(t *testing.T) {
	mux := NewServeMux()
	mux.Handle("GET /sniff", FromStd(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Before", "1")
		w.WriteHeader(201)
		w.Header().Set("X-After", "ignored") // like net/http, changes after WriteHeader are not sent
		_, _ = io.WriteString(w, "<html>")
		_, _ = io.WriteString(w, "<body>hi</body></html>")
	})))
	mux.Handle("GET /notype", FromStd(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header()["Content-Type"] = nil // explicitly unset: no sniffing
		_, _ = io.WriteString(w, "<html>")
	})))
	mux.Handle("GET /trailers", FromStd(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Trailer", "X-Declared")
		w.WriteHeader(200)
		_, _ = io.WriteString(w, "body")
		w.Header().Set("X-Declared", "d")
		w.Header().Set(http.TrailerPrefix+"X-Prefixed", "p")
	})))
	mux.Handle("GET /stream", FromStd(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "first")
		if err := http.NewResponseController(w).Flush(); err != nil {
			t.Error(err)
		}
		<-r.Context().Done() // the client closes the body after reading the first chunk
	})))
	mux.Handle("GET /early", FromStd(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Link", "</style.css>; rel=preload")
		w.WriteHeader(103)
		w.Header().Del("Link")
		_, _ = io.WriteString(w, "done")
	})))
	for _, c := range startAllProtocols(t, mux) {
		resp, err := c.client.Get(c.base + "/sniff") //nolint:bodyclose // readAll closes the body
		body := readAll(t, resp, err)
		if resp.StatusCode != 201 || body != "<html><body>hi</body></html>" ||
			resp.Header.Get("Content-Type") != "text/html; charset=utf-8" || resp.Header.Get("X-Before") != "1" ||
			resp.Header.Get("X-After") != "" {
			t.Errorf("%s sniff: %d %q %v", c.name, resp.StatusCode, body, resp.Header)
		}

		resp, err = c.client.Get(c.base + "/notype") //nolint:bodyclose // readAll closes the body
		if readAll(t, resp, err); resp.Header.Get("Content-Type") != "" {
			t.Errorf("%s notype: %v", c.name, resp.Header)
		}

		resp, err = c.client.Get(c.base + "/trailers") //nolint:bodyclose // readAll closes the body
		if body := readAll(t, resp, err); body != "body" || resp.Trailer.Get("X-Declared") != "d" ||
			resp.Trailer.Get("X-Prefixed") != "p" {
			t.Errorf("%s trailers: %q %v", c.name, body, resp.Trailer)
		}

		resp, err = c.client.Get(c.base + "/stream")
		if err != nil {
			t.Fatal(err)
		}
		buf := make([]byte, 5)
		if _, err := io.ReadFull(resp.Body, buf); err != nil || string(buf) != "first" {
			t.Errorf("%s stream: %q %v", c.name, buf, err)
		}
		_ = resp.Body.Close()

		resp, err = c.client.Get(c.base + "/early") //nolint:bodyclose // readAll closes the body
		if body := readAll(t, resp, err); body != "done" || resp.StatusCode != 200 || resp.Header.Get("Link") != "" {
			t.Errorf("%s early hints: %d %q %v", c.name, resp.StatusCode, body, resp.Header)
		}
	}
}

func TestFromStdHijack(t *testing.T) {
	ts := startServer(t, FromStd(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, rw, err := http.NewResponseController(w).Hijack()
		if err != nil {
			t.Error(err)
			return
		}
		defer func() { _ = conn.Close() }()
		_, _ = rw.WriteString("HTTP/1.1 200 OK\r\nContent-Length: 8\r\nConnection: close\r\n\r\nhijacked")
		_ = rw.Flush()
	})).ServeHTTP)
	resp, err := newClient(t).Get(ts.url("/")) //nolint:bodyclose // readAll closes the body
	if body := readAll(t, resp, err); body != "hijacked" {
		t.Fatalf("body %q", body)
	}
}

func TestFromStdBodyUnusableAfterReturn(t *testing.T) {
	kept := make(chan *http.Request, 1)
	ts := startServer(t, FromStd(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		kept <- r
	})).ServeHTTP)
	resp, err := newClient(t).Post(ts.url("/x?a=1"), "text/plain", strings.NewReader("body")) //nolint:bodyclose // readAll closes the body
	readAll(t, resp, err)
	r := <-kept
	// The request itself stays valid, like in net/http.
	if r.URL.Path != "/x" || r.URL.RawQuery != "a=1" || r.Header.Get("Content-Type") != "text/plain" {
		t.Fatalf("kept request: %v %v", r.URL, r.Header)
	}
	if _, err := r.Body.Read(make([]byte, 1)); !errors.Is(err, http.ErrBodyReadAfterClose) {
		t.Fatalf("read after return: %v", err)
	}
	if r.Context().Err() == nil {
		t.Fatal("context not canceled after return")
	}
}

func TestFromStdAbortHandler(t *testing.T) {
	ts := startServer(t, FromStd(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		panic(http.ErrAbortHandler)
	})).ServeHTTP)
	_, err := newClient(t).Get(ts.url("/")) //nolint:bodyclose // the request fails
	if err == nil {
		t.Fatal("aborted request succeeded")
	}
	if logs := ts.logs.String(); strings.Contains(logs, "panic") {
		t.Fatalf("ErrAbortHandler was logged: %s", logs)
	}
}

func TestToStd(t *testing.T) {
	type seen struct {
		method, path, query, host, header, body, proto, id, pattern string
		remote                                                      bool
	}
	got := make(chan seen, 1)
	mux := http.NewServeMux()
	mux.Handle("POST /items/{id}", ToStd(HandlerFunc(func(w ResponseWriter, r *Request) {
		body, _ := io.ReadAll(r.Body)
		got <- seen{
			method: r.Method(), path: string(r.Path()), query: r.URI().Query().Get("q"), host: string(r.Host()),
			header: r.Header.Get("X-Test"), body: string(body), proto: r.Proto(), id: r.PathValue("id"),
			pattern: r.Pattern(), remote: r.RemoteAddr() != nil,
		}
		w.Header().Set("Content-Type", "text/plain")
		w.Header().Add("Vary", "A")
		w.Header().Add("Vary", "B")
		w.WriteHeader(202)
		_, _ = w.WriteString("ok")
		w.(TrailerSetter).Trailer().Set("X-Trailer", "t")
	})))
	mux.Handle("GET /stream", ToStd(HandlerFunc(func(w ResponseWriter, r *Request) {
		_, _ = w.WriteString("first")
		if err := w.Flush(); err != nil {
			t.Error(err)
		}
		<-r.Context().Done() // canceled when the client goes away
	})))
	mux.Handle("GET /hijack", ToStd(HandlerFunc(func(w ResponseWriter, r *Request) {
		conn, rw, err := w.(Hijacker).Hijack()
		if err != nil {
			t.Error(err)
			return
		}
		defer func() { _ = conn.Close() }()
		_, _ = rw.WriteString("HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nhj")
		_ = rw.Flush()
	})))
	srv := httptest.NewServer(mux)
	defer srv.Close()

	req, _ := http.NewRequest("POST", srv.URL+"/items/a%20b?q=1", strings.NewReader("payload"))
	req.Header.Set("X-Test", "v")
	resp, err := srv.Client().Do(req) //nolint:bodyclose // readAll closes the body
	body := readAll(t, resp, err)
	if resp.StatusCode != 202 || body != "ok" || resp.Header.Get("Content-Type") != "text/plain" ||
		strings.Join(resp.Header["Vary"], ",") != "A,B" || resp.Trailer.Get("X-Trailer") != "t" {
		t.Fatalf("response: %d %q %v trailer %v", resp.StatusCode, body, resp.Header, resp.Trailer)
	}
	want := seen{"POST", "/items/a b", "1", req.URL.Host, "v", "payload", "HTTP/1.1", "a b", "POST /items/{id}", true}
	if s := <-got; s != want {
		t.Fatalf("\n got %+v\nwant %+v", s, want)
	}

	resp, err = srv.Client().Get(srv.URL + "/stream")
	if err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 5)
	if _, err := io.ReadFull(resp.Body, buf); err != nil || string(buf) != "first" {
		t.Fatalf("stream: %q %v", buf, err)
	}
	_ = resp.Body.Close()

	resp, err = srv.Client().Get(srv.URL + "/hijack") //nolint:bodyclose // readAll closes the body
	if body := readAll(t, resp, err); body != "hj" {
		t.Fatalf("hijack: %q", body)
	}
}

func TestToStdPanics(t *testing.T) {
	for _, tt := range []struct {
		name string
		v    any
		want any
	}{
		{"abort", ErrAbortHandler, http.ErrAbortHandler},
		{"other", "boom", "boom"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			h := ToStd(HandlerFunc(func(w ResponseWriter, r *Request) { panic(tt.v) }))
			defer func() {
				if v := recover(); v != tt.want {
					t.Fatalf("recovered %v, want %v", v, tt.want)
				}
			}()
			h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/", nil))
		})
	}
}

func TestStdAdaptersUnwrap(t *testing.T) {
	q := HandlerFunc(func(ResponseWriter, *Request) {})
	if h, ok := FromStd(ToStd(q)).(HandlerFunc); !ok || h == nil {
		t.Fatal("FromStd(ToStd(h)) is not h")
	}
	s := http.NotFoundHandler()
	if ToStd(FromStd(s)) == nil {
		t.Fatal("nil handler")
	}
	if _, ok := ToStd(FromStd(s)).(toStdHandler); ok {
		t.Fatal("ToStd(FromStd(h)) is wrapped")
	}
}

type ctxKey struct{}

func TestFromStdMiddlewareAndChain(t *testing.T) {
	var order []string
	var mu sync.Mutex
	record := func(s string) {
		mu.Lock()
		order = append(order, s)
		mu.Unlock()
	}
	stdMW := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			record("std")
			w.Header().Set("X-Std", "1")
			next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), ctxKey{}, "from std")))
		})
	}
	qilatMW := func(name string) func(Handler) Handler {
		return func(next Handler) Handler {
			return HandlerFunc(func(w ResponseWriter, r *Request) {
				record(name)
				next.ServeHTTP(w, r)
			})
		}
	}
	mux := NewServeMux()
	mux.HandleFunc("GET /users/{id}", func(w ResponseWriter, r *Request) {
		_, _ = w.WriteString(r.PathValue("id") + " " + r.Context().Value(ctxKey{}).(string))
	})
	h := Chain(qilatMW("a"), FromStdMiddleware(stdMW), qilatMW("b"))(mux)
	for _, c := range startAllProtocols(t, h) {
		mu.Lock()
		order = nil
		mu.Unlock()
		resp, err := c.client.Get(c.base + "/users/42") //nolint:bodyclose // readAll closes the body
		if body := readAll(t, resp, err); body != "42 from std" || resp.Header.Get("X-Std") != "1" {
			t.Errorf("%s: %q %v", c.name, body, resp.Header)
		}
		mu.Lock()
		if got := strings.Join(order, ","); got != "a,std,b" {
			t.Errorf("%s: order %s", c.name, got)
		}
		mu.Unlock()
	}
}

func TestStripPrefix(t *testing.T) {
	inner := NewServeMux()
	inner.HandleFunc("/{rest...}", func(w ResponseWriter, r *Request) {
		_, _ = w.WriteString(string(r.Path()) + "|" + string(r.URI().RawQuery()) + "|" + r.PathValue("rest"))
	})
	outer := NewServeMux()
	outerValues := make(chan string, 3)
	outer.HandleFunc("/static/{tail...}", func(w ResponseWriter, r *Request) {
		want := r.PathValue("tail")
		StripPrefix("/static", inner).ServeHTTP(w, r)
		if got := r.PathValue("tail"); got != want {
			outerValues <- got
		}
	})
	ts := startServer(t, outer.ServeHTTP)
	client := newClient(t)
	for _, tt := range []struct{ path, want string }{
		{"/static/a/b?x=1", "/a/b|x=1|a/b"},
		{"/static/a%2Fb", "/a/b||a/b"},
		{"/static/", "/||"},
	} {
		resp, err := client.Get(ts.url(tt.path)) //nolint:bodyclose // readAll closes the body
		if body := readAll(t, resp, err); body != tt.want {
			t.Errorf("%s: %q, want %q", tt.path, body, tt.want)
		}
	}
	if len(outerValues) > 0 {
		t.Errorf("inner mux changed the outer path value to %q", <-outerValues)
	}
	resp, err := client.Get(ts.url("/st%61tic/x")) //nolint:bodyclose // readAll closes the body
	if readAll(t, resp, err); resp.StatusCode != 404 {
		t.Errorf("escaped prefix: %d", resp.StatusCode)
	}
}

// discardStdWriter is a minimal net/http ResponseWriter for benchmarks.
type discardStdWriter struct{ h http.Header }

func (w *discardStdWriter) Header() http.Header         { return w.h }
func (w *discardStdWriter) Write(p []byte) (int, error) { return len(p), nil }
func (w *discardStdWriter) WriteHeader(int)             {}

func benchmarkServe(b *testing.B, h Handler) {
	cfg := &exchangeConfig{handler: h}
	method, target, host := []byte("GET"), []byte("/plaintext"), []byte("bench")
	headers := [][2][]byte{
		{[]byte("user-agent"), []byte("bench/1.0")},
		{[]byte("accept"), []byte("text/plain")},
		{[]byte("accept-encoding"), []byte("gzip")},
	}
	b.ReportAllocs()
	for b.Loop() {
		x := acquireExchange(cfg)
		_ = x.SetMethod(method)
		_ = x.SetTarget(target)
		_ = x.SetHost(host)
		for _, h := range headers {
			_ = x.AddHeader(h[0], h[1])
		}
		_ = x.Serve(discardSink{})
		x.Release()
	}
}

var benchBody = []byte("Hello, World!")

func BenchmarkAdapterNative(b *testing.B) {
	benchmarkServe(b, HandlerFunc(func(w ResponseWriter, r *Request) {
		w.Header().Set("Content-Type", "text/plain")
		_, _ = w.Write(benchBody)
	}))
}

func BenchmarkAdapterFromStd(b *testing.B) {
	benchmarkServe(b, FromStd(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		_, _ = w.Write(benchBody)
	})))
}

func BenchmarkAdapterFromStdMiddleware(b *testing.B) {
	mw := FromStdMiddleware(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { next.ServeHTTP(w, r) })
	})
	benchmarkServe(b, mw(HandlerFunc(func(w ResponseWriter, r *Request) {
		w.Header().Set("Content-Type", "text/plain")
		_, _ = w.Write(benchBody)
	})))
}

func BenchmarkAdapterToStd(b *testing.B) {
	h := ToStd(HandlerFunc(func(w ResponseWriter, r *Request) {
		w.Header().Set("Content-Type", "text/plain")
		_, _ = w.Write(benchBody)
	}))
	req := httptest.NewRequest("GET", "/plaintext", nil)
	req.Header.Set("User-Agent", "bench/1.0")
	w := &discardStdWriter{h: make(http.Header)}
	b.ReportAllocs()
	for b.Loop() {
		clear(w.h)
		h.ServeHTTP(w, req)
	}
}
