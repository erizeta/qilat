package qilat

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
)

// muxResult is what a mux produced for one request, comparable across net/http and qilat.
type muxResult struct {
	status                   int
	location, allow, ctype   string
	body                     string
	handlerPattern, matchPat string
}

func (r muxResult) String() string {
	return fmt.Sprintf("status=%d location=%q allow=%q ctype=%q pattern=%q handler-pattern=%q body=%q",
		r.status, r.location, r.allow, r.ctype, r.matchPat, r.handlerPattern, r.body)
}

// patternNames returns the wildcard names of a pattern, so handlers can report every path value.
func patternNames(t testing.TB, p string) []string {
	pat, err := parsePattern(p)
	if err != nil {
		t.Fatalf("parsePattern(%q): %v", p, err)
	}
	return pat.names
}

type muxRequest struct{ method, target, host string }

func (rq muxRequest) String() string { return rq.method + " " + rq.target + " (host " + rq.host + ")" }

// stdMux runs rq through net/http.ServeMux. ok is false if net/http rejects the request.
func stdMux(t testing.TB, patterns []string, rq muxRequest) (muxResult, bool) {
	mux := http.NewServeMux()
	for _, p := range patterns {
		names := patternNames(t, p)
		mux.HandleFunc(p, func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/plain") // net/http would sniff it; qilat does not
			_, _ = fmt.Fprintf(w, "pattern=%s", r.Pattern)
			for _, n := range names {
				_, _ = fmt.Fprintf(w, " %s=%q", n, r.PathValue(n))
			}
		})
	}
	raw := rq.method + " " + rq.target + " HTTP/1.1\r\nHost: " + rq.host + "\r\n\r\n"
	req, err := http.ReadRequest(bufio.NewReader(strings.NewReader(raw)))
	if err != nil {
		return muxResult{}, false
	}
	_, handlerPattern := mux.Handler(req)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	body := rec.Body.String()
	if req.Method == "HEAD" { // the recorder keeps a body a real server would not send
		body = ""
	}
	return muxResult{
		status:         rec.Code,
		location:       rec.Header().Get("Location"),
		allow:          rec.Header().Get("Allow"),
		ctype:          rec.Header().Get("Content-Type"),
		body:           body,
		handlerPattern: handlerPattern,
		matchPat:       req.Pattern,
	}, true
}

// qilatMux runs rq through qilat's ServeMux. ok is false if qilat rejects the request.
func qilatMux(t testing.TB, patterns []string, rq muxRequest) (muxResult, bool) {
	mux := NewServeMux()
	for _, p := range patterns {
		names := patternNames(t, p)
		mux.HandleFunc(p, func(w ResponseWriter, r *Request) {
			w.Header().Set("Content-Type", "text/plain")
			_, _ = fmt.Fprintf(w, "pattern=%s", r.Pattern())
			for _, n := range names {
				_, _ = fmt.Fprintf(w, " %s=%q", n, r.PathValue(n))
			}
		})
	}
	var res muxResult
	h := HandlerFunc(func(w ResponseWriter, r *Request) {
		_, res.handlerPattern = mux.Handler(r)
		mux.ServeHTTP(w, r)
		res.matchPat = r.Pattern()
	})
	x := acquireExchange(&exchangeConfig{handler: h})
	defer x.Release()
	if x.SetMethod([]byte(rq.method)) != nil || x.SetTarget([]byte(rq.target)) != nil || x.SetHost([]byte(rq.host)) != nil {
		return muxResult{}, false
	}
	sink := &fakeSink{}
	if err := x.Serve(sink); err != nil {
		t.Fatalf("%s: Serve: %v", rq, err)
	}
	res.status = sink.status
	res.body = sink.body.String()
	for _, f := range sink.header {
		name, value, _ := strings.Cut(f, "=")
		switch name {
		case "location":
			res.location = value
		case "allow":
			res.allow = value
		case "content-type":
			res.ctype = value
		}
	}
	return res, true
}

// sameMuxResult compares two results. When net/http mangles escapes in redirect targets, a redirect whose
// target holds one is compared without its Location and the body that repeats it.
func sameMuxResult(got, want muxResult) bool {
	if !stdRedirectEscaping && strings.Contains(got.location+want.location, "%") {
		got.location, got.body, want.location, want.body = "", "", "", ""
	}
	return got == want
}

func checkMux(t *testing.T, patterns []string, rq muxRequest) {
	t.Helper()
	want, ok := stdMux(t, patterns, rq)
	if !ok {
		t.Fatalf("%s: net/http rejected the request", rq)
	}
	got, ok := qilatMux(t, patterns, rq)
	if !ok {
		t.Fatalf("%s: qilat rejected the request", rq)
	}
	if !sameMuxResult(got, want) {
		t.Errorf("%s with patterns %q:\n got %v\nwant %v", rq, patterns, got, want)
	}
}

var muxTables = []struct {
	name     string
	patterns []string
	requests []string // "METHOD target [host]"
}{
	{
		name:     "static and subtree",
		patterns: []string{"/", "/a", "/a/", "/a/b/c", "/images/", "/images/thumbnails/"},
		requests: []string{
			"GET /", "GET /a", "GET /a/", "GET /a/x", "GET /a/b/c", "GET /a/b/c/", "GET /images",
			"GET /images/x.png", "GET /images/thumbnails", "GET /images/thumbnails/x.png", "POST /zzz",
		},
	},
	{
		name: "methods",
		patterns: []string{
			"GET /items", "POST /items", "DELETE /items/{id}", "PUT /items/{id}", "/other",
			"GET /x/{$}", "PATCH /x/", "HEAD /h",
		},
		requests: []string{
			"GET /items", "HEAD /items", "POST /items", "PUT /items", "OPTIONS /items", "GET /items/1",
			"DELETE /items/1", "PUT /items/1", "PATCH /items/1", "BREW /other", "GET /x/", "GET /x",
			"PATCH /x/y", "GET /x/y", "HEAD /h", "GET /h", "get /items",
		},
	},
	{
		name: "wildcards",
		patterns: []string{
			"/users/{id}", "/users/{id}/posts/{post}", "/users/me", "/files/{path...}", "/b/{bucket}/o/{obj...}",
			"/exact/{$}", "/q/{a}/{b}/{c}/{d}",
		},
		requests: []string{
			"GET /users/42", "GET /users/me", "GET /users/42/posts/7", "GET /users/42/posts", "GET /users/",
			"GET /users", "GET /files", "GET /files/", "GET /files/a/b/c.txt", "GET /files/a%2Fb",
			"GET /b/bk/o/x/y/z", "GET /b/bk/o/", "GET /b/bk/o", "GET /exact/", "GET /exact", "GET /exact/x",
			"GET /users/%E4%BD%A0", "GET /users/a%20b", "GET /users/a%2Fb", "GET /q/1/2/3/4", "GET /q/1/2/3/4/",
			"GET /users/a%2Fb/posts/c%25d",
		},
	},
	{
		name:     "escaped literals",
		patterns: []string{"/a%2Fb/", "/a/b/", "/%61bc", "/x/{$}", "/caf%C3%A9"},
		requests: []string{
			"GET /a%2Fb/", "GET /a%2fb/x", "GET /a/b/", "GET /a/b/x", "GET /abc", "GET /%61bc", "GET /x/%2F",
			"GET /x/", "GET /caf%C3%A9", "GET /café", "GET /a%2Fb",
		},
	},
	{
		name:     "hosts",
		patterns: []string{"/", "example.com/", "example.com/admin/{x}", "GET api.example.com/v1/{rest...}", "/admin/{x}"},
		requests: []string{
			"GET / example.com", "GET /x example.com:8080", "GET /admin/1 example.com", "GET /admin/1 other.com",
			"GET /v1/a/b api.example.com", "POST /v1/a/b api.example.com", "GET /v1 api.example.com",
			"GET / [::1]:80", "GET / EXAMPLE.COM", "GET / a:b:c", "GET http://example.com/admin/2 other.com",
		},
	},
	{
		name:     "unclean paths",
		patterns: []string{"/", "/a/b", "/a/", "GET /c/{x}"},
		requests: []string{
			"GET /a/../a/b", "GET /a//b", "GET /./a/b", "GET /a/b/.", "GET /a/b/..", "GET //", "GET /a/./?q=1",
			"GET /a/%2e%2e/b", "POST /a/../a/b?x=y", "GET /c/./d", "GET /a/b/../../c/z", "GET /..",
		},
	},
	{
		name:     "trailing slash redirect with query",
		patterns: []string{"/dir/", "POST /post/", "/both", "/both/", "/w/{x}/"},
		requests: []string{
			"GET /dir?a=1&b=2", "HEAD /dir", "POST /dir", "POST /post", "GET /post", "GET /both", "GET /w/1",
			"GET /w/1?q", "GET /dir/?a=1", "GET /d%C3%A9/",
		},
	},
	{
		name:     "connect",
		patterns: []string{"CONNECT example.com/", "/", "CONNECT /tunnel/"},
		requests: []string{
			"CONNECT example.com:443 example.com:443", "CONNECT other:443 other:443", "CONNECT /tunnel x",
			"CONNECT /tunnel/a//b x", "CONNECT /a/../b x",
		},
	},
	{
		name: "many siblings",
		patterns: []string{
			"/s/a", "/s/b", "/s/c", "/s/d", "/s/e", "/s/f", "/s/g", "/s/h", "/s/i", "/s/j", "/s/{x}",
			"GET /m", "POST /m", "PUT /m", "PATCH /m", "DELETE /m", "OPTIONS /m", "TRACE /m", "BREW /m", "MKCOL /m",
			"LOCK /m", "a.example/", "b.example/", "c.example/",
		},
		requests: []string{
			"GET /s/a", "GET /s/j", "GET /s/k", "GET /s/", "HEAD /m", "MKCOL /m", "LOCK /m", "UNLOCK /m",
			"GET /m/", "GET / b.example", "GET / d.example",
		},
	},
	{
		name:     "no match",
		patterns: []string{"GET /only/{x}", "POST /p"},
		requests: []string{"GET /", "GET /only", "GET /only/", "POST /only/1", "GET /p", "PUT /p/"},
	},
}

func TestServeMuxMatchesNetHTTP(t *testing.T) {
	for _, tt := range muxTables {
		t.Run(tt.name, func(t *testing.T) {
			for _, line := range tt.requests {
				f := strings.Fields(line)
				rq := muxRequest{method: f[0], target: f[1], host: "example.org"}
				if len(f) > 2 {
					rq.host = f[2]
				}
				checkMux(t, tt.patterns, rq)
			}
		})
	}
}

// net/http redirects an absolute-form target with an empty path to "/". qilat's URI already treats the
// empty path as "/" (RFC 9110 section 4.2.3), so the request is served directly.
func TestServeMuxAbsoluteFormEmptyPath(t *testing.T) {
	got, ok := qilatMux(t, []string{"example.com/"}, muxRequest{"GET", "http://example.com", "other.com"})
	if !ok || got.status != 200 || got.matchPat != "example.com/" {
		t.Fatalf("got %v", got)
	}
}

func TestServeMuxPatternsMatchNetHTTP(t *testing.T) {
	// Each list is registered in order; the outcome (panic or not) must match net/http at every step.
	lists := [][]string{
		{"/a", "/a"},
		{"GET /a", "/a"},
		{"GET /", "/index.html"},
		{"/a/{x}", "/{y}/b"},
		{"/a/{x}", "/a/b"},
		{"/{x}/{y}", "/a/{z}", "/{w}/b"},
		{"/a/", "/a/{x...}"},
		{"/a/{$}", "/a/"},
		{"/a/{$}", "/a/{x}"},
		{"GET /a", "HEAD /a"},
		{"HEAD /a/{x}", "GET /a/b"},
		{"POST /a/{x}", "/a/b"},
		{"example.com/a", "/a"},
		{"example.com/a", "example.com/a"},
		{"a.com/", "b.com/"},
		{"/a/{x}/c", "/a/b/{y}"},
		{"/{x...}", "/"},
		{"/a/b", "/a/{x}", "/{x}/b", "/{x}/{y}"},
		{"/static/{$}", "/static/x", "/static/{file}", "/static/{rest...}"},
		{"CONNECT /a//b", "/a/b"},
		{"/{x}/b/c", "/a/{y}/{z}"},
		{"/{x}/b/c", "/a/{y}/d"},
		{"/a/{x...}", "/{y}/b/"},
		{"/a/b/{x...}", "/{y}/b"},
		{"/{x}/b/", "/a/{y...}"},
		{"/a/{x}", "/a/{x}/c/"},
		{"GET /a/{x}", "/a/b"},
		{"/a/{x}", "HEAD /a/b"},
		{"/{x}/{y...}", "/a/b/{$}"},
		// Invalid patterns.
		{""}, {"GET"}, {"/{"}, {"/a{x}"}, {"/{x}y"}, {"/{x...}/a"}, {"/{$}/a"}, {"/{}"}, {"/{...}"},
		{"/{1x}"}, {"/{x}/{x}"}, {"GE T /"}, {"G\x01T /"}, {"host{x}/"}, {"GET /a/../b"}, {"GET //a"},
		{"/a/../b"}, {"GET\t\t /tabs"}, {"/{héllo}"}, {"/{x$}"},
	}
	for _, list := range lists {
		std, q := http.NewServeMux(), NewServeMux()
		for i, p := range list {
			stdPanic := panics(func() { std.HandleFunc(p, func(http.ResponseWriter, *http.Request) {}) })
			qPanic := panics(func() { q.HandleFunc(p, func(ResponseWriter, *Request) {}) })
			if stdPanic != qPanic {
				t.Errorf("%q: registering %q: net/http panic=%v, qilat panic=%v", list[:i], p, stdPanic, qPanic)
			}
		}
	}
}

func panics(f func()) (p bool) {
	defer func() { p = recover() != nil }()
	f()
	return false
}

func TestServeMuxConflictMessage(t *testing.T) {
	mux := NewServeMux()
	mux.HandleFunc("/a/{x}", func(ResponseWriter, *Request) {})
	defer func() {
		msg := fmt.Sprint(recover())
		for _, want := range []string{`"/{y}/b"`, `"/a/{x}"`, "servemux_test.go:", `"/a/b"`} {
			if !strings.Contains(msg, want) {
				t.Errorf("conflict message %q does not contain %q", msg, want)
			}
		}
	}()
	mux.HandleFunc("/{y}/b", func(ResponseWriter, *Request) {})
}

func TestServeMuxNilHandler(t *testing.T) {
	mux := NewServeMux()
	for name, f := range map[string]func(){
		"Handle nil":            func() { mux.Handle("/", nil) },
		"Handle nil func":       func() { mux.Handle("/", HandlerFunc(nil)) },
		"HandleFunc nil":        func() { mux.HandleFunc("/", nil) },
		"Handle empty pattern":  func() { mux.Handle("", HandlerFunc(func(ResponseWriter, *Request) {})) },
		"Handle invalid method": func() { mux.Handle("G(T /", HandlerFunc(func(ResponseWriter, *Request) {})) },
	} {
		if !panics(f) {
			t.Errorf("%s: no panic", name)
		}
	}
}

func TestServeMuxAsterisk(t *testing.T) {
	mux := NewServeMux()
	mux.HandleFunc("/", func(w ResponseWriter, r *Request) { t.Error("handler called for *") })
	for _, tt := range []struct {
		minor     int
		wantClose bool
	}{{1, true}, {0, false}} {
		sink, err := serve(t, request{method: "OPTIONS", target: "*", major: 1, minor: tt.minor}, mux.ServeHTTP)
		if err != nil {
			t.Fatal(err)
		}
		if sink.status != 400 || slices.Contains(sink.header, "connection=close") != tt.wantClose {
			t.Errorf("HTTP/1.%d: status %d, header %q", tt.minor, sink.status, sink.header)
		}
	}
}

func TestPathValues(t *testing.T) {
	mux := NewServeMux()
	mux.HandleFunc("/b/{bucket}/o/{obj...}", func(w ResponseWriter, r *Request) {
		if got := r.PathValue("bucket"); got != "a/b" {
			t.Errorf("bucket = %q", got)
		}
		if got := r.PeekPathValue("obj"); string(got) != "x y/z" {
			t.Errorf("obj = %q", got)
		}
		if got := r.PeekPathValue("missing"); got != nil {
			t.Errorf("missing = %q, want nil", got)
		}
		r.SetPathValue("bucket", "%41")
		r.SetPathValue("extra", "e")
		r.SetPathValue("extra", "e2")
		if r.PathValue("bucket") != "%41" || r.PathValue("extra") != "e2" {
			t.Errorf("after SetPathValue: bucket=%q extra=%q", r.PathValue("bucket"), r.PathValue("extra"))
		}
		c := r.Clone(r.Context())
		r.SetPathValue("bucket", "changed")
		if c.PathValue("bucket") != "%41" || c.PathValue("extra") != "e2" || c.Pattern() != "/b/{bucket}/o/{obj...}" {
			t.Errorf("clone: bucket=%q extra=%q pattern=%q", c.PathValue("bucket"), c.PathValue("extra"), c.Pattern())
		}
	})
	mux.HandleFunc("/empty/{x}/", func(w ResponseWriter, r *Request) {
		if v := r.PeekPathValue("x"); v == nil || len(v) != 0 {
			t.Errorf("empty wildcard value = %#v, want empty non-nil", v)
		}
	})
	if _, err := serve(t, request{target: "/b/a%2Fb/o/x%20y/z"}, mux.ServeHTTP); err != nil {
		t.Fatal(err)
	}
	// Values and the pattern must not leak into the next request served by the same pooled exchange.
	if _, err := serve(t, request{target: "/plain"}, func(w ResponseWriter, r *Request) {
		if r.Pattern() != "" || r.PathValue("bucket") != "" || r.PathValue("extra") != "" {
			t.Errorf("leaked: pattern=%q bucket=%q extra=%q", r.Pattern(), r.PathValue("bucket"), r.PathValue("extra"))
		}
	}); err != nil {
		t.Fatal(err)
	}
}

func TestNestedServeMux(t *testing.T) {
	inner := NewServeMux()
	inner.HandleFunc("/api/{version}/{name}", func(w ResponseWriter, r *Request) {
		if r.PathValue("version") != "v1" || r.PathValue("name") != "n" || r.PathValue("outer") != "" ||
			r.PathValue("tenant") != "t1" || r.Pattern() != "/api/{version}/{name}" {
			t.Errorf("inner: pattern=%q version=%q name=%q outer=%q tenant=%q", r.Pattern(), r.PathValue("version"),
				r.PathValue("name"), r.PathValue("outer"), r.PathValue("tenant"))
		}
	})
	outer := NewServeMux()
	outer.Handle("/api/{outer...}", HandlerFunc(func(w ResponseWriter, r *Request) {
		r.SetPathValue("tenant", "t1")
		inner.ServeHTTP(w, r)
	}))
	if _, err := serve(t, request{target: "/api/v1/n"}, outer.ServeHTTP); err != nil {
		t.Fatal(err)
	}
}

func TestServeMuxAllocs(t *testing.T) {
	if raceEnabled {
		t.Skip("allocation counts are not meaningful with the race detector")
	}
	mux := benchMux()
	for _, target := range []string{
		"/", "/users/42", "/users/42/posts/7", "/files/a/b/c", "/static/app.js", "/u/%41", "/files/x%2Fy/z",
	} {
		// A fresh exchange per target, so buffers grown by an earlier target cannot hide an allocation.
		x := new(serverExchange)
		x.cfg = &exchangeConfig{handler: mux}
		x.req.init(context.Background())
		_ = x.SetMethod([]byte("GET"))
		_ = x.SetTarget([]byte(target))
		_ = x.SetHost([]byte("example.com"))
		w := &nopWriter{}
		if n := testing.AllocsPerRun(200, func() { mux.ServeHTTP(w, &x.req) }); n != 0 {
			t.Errorf("%s: allocs = %v, want 0", target, n)
		}
	}
}

func TestStripHostPort(t *testing.T) {
	for _, h := range []string{
		"", "example.com", "example.com:80", "example.com:", "[::1]:80", "[::1]", "::1", "a:b:c", "[::1]x:80",
		"[a]:80]", "a[b]:80", "[a:80", ":80",
	} {
		want := h
		if strings.Contains(h, ":") {
			if host, _, err := net.SplitHostPort(h); err == nil {
				want = host
			}
		}
		if got := string(stripHostPort([]byte(h))); got != want {
			t.Errorf("stripHostPort(%q) = %q, want %q", h, got, want)
		}
	}
}

func TestIsCleanPath(t *testing.T) {
	for _, p := range []string{
		"", "/", "//", "/a", "/a/", "/a//", "/a/.", "/a/..", "/./a", "/../a", "/.a", "/a./", "/..a/b", "a",
		"/a/b/c/", "/a/%2e/", "/...", "/a/.../",
	} {
		if got, want := isCleanPath([]byte(p)), cleanPath(p) == p; got != want {
			t.Errorf("isCleanPath(%q) = %v, want %v", p, got, want)
		}
	}
}

func FuzzServeMux(f *testing.F) {
	patterns := []string{
		"/", "/a/", "/a/b", "GET /users/{id}", "POST /users/{id}", "GET /users/me", "/files/{path...}", "/x/{$}",
		"example.com/admin/{x}", "/%61%2Fb/", "DELETE /items/{id}/tags/{tag}",
	}
	for _, s := range []string{"/", "/a", "/a/./b", "/users/1", "/files/a%2Fb/c", "/x/", "/a%2Fb", "/items/1/tags/2"} {
		f.Add("GET", s, "example.com")
	}
	f.Fuzz(func(t *testing.T, method, target, host string) {
		if !strings.HasPrefix(target, "/") || strings.ContainsAny(method+target+host, "\r\n") {
			t.Skip()
		}
		// For a path holding both escapes and bytes net/url would escape, net/http re-encodes the decoded path,
		// so %2F splits segments and %2e%2e is cleaned like "..". qilat keeps escapes opaque, as both document.
		p, _, _ := strings.Cut(target, "?")
		if strings.Contains(p, "%") && strings.ContainsFunc(p, func(c rune) bool { return c > 0x7f || pathBytes[byte(c)]&pathPchar == 0 }) {
			t.Skip()
		}
		rq := muxRequest{method: method, target: target, host: host}
		want, ok1 := stdMux(t, patterns, rq)
		got, ok2 := qilatMux(t, patterns, rq)
		if !ok1 || !ok2 {
			t.Skip()
		}
		if !sameMuxResult(got, want) {
			t.Errorf("%s:\n got %v\nwant %v", rq, got, want)
		}
	})
}

var benchPatterns = []string{
	"GET /{$}", "GET /users/{id}", "GET /users/{id}/posts/{post}", "POST /users", "GET /files/{path...}",
	"GET /static/app.js", "GET /static/app.css", "GET /health", "GET /u/{name}", "DELETE /users/{id}",
}

func benchMux() *ServeMux {
	mux := NewServeMux()
	h := func(w ResponseWriter, r *Request) { _ = r.PeekPathValue("id") }
	for _, p := range benchPatterns {
		mux.HandleFunc(p, h)
	}
	return mux
}

// BenchmarkServeMux measures only dispatch: both muxes get a prepared request and a no-op writer.
func BenchmarkServeMux(b *testing.B) {
	for _, target := range []string{"/static/app.js", "/users/42", "/users/42/posts/7", "/files/a/b/c", "/nope"} {
		name := strings.ReplaceAll(target[1:], "/", "_")
		b.Run("qilat/"+name, func(b *testing.B) {
			mux := benchMux()
			x := acquireExchange(&exchangeConfig{handler: mux})
			defer x.Release()
			_ = x.SetMethod([]byte("GET"))
			_ = x.SetTarget([]byte(target))
			_ = x.SetHost([]byte("example.com"))
			w := &nopWriter{}
			b.ReportAllocs()
			for b.Loop() {
				mux.ServeHTTP(w, &x.req)
			}
		})
		b.Run("nethttp/"+name, func(b *testing.B) {
			mux := http.NewServeMux()
			for _, p := range benchPatterns {
				mux.HandleFunc(p, func(w http.ResponseWriter, r *http.Request) { _ = r.PathValue("id") })
			}
			req := httptest.NewRequest("GET", target, nil)
			w := discardWriter{h: http.Header{}}
			b.ReportAllocs()
			for b.Loop() {
				mux.ServeHTTP(w, req)
			}
		})
	}
}

type nopWriter struct{ h Header }

func (w *nopWriter) Header() *Header                   { w.h.Reset(); return &w.h }
func (w *nopWriter) WriteHeader(int)                   {}
func (w *nopWriter) Write(p []byte) (int, error)       { return len(p), nil }
func (w *nopWriter) WriteString(s string) (int, error) { return len(s), nil }
func (w *nopWriter) Flush() error                      { return nil }

type discardWriter struct{ h http.Header }

func (w discardWriter) Header() http.Header         { return w.h }
func (w discardWriter) Write(p []byte) (int, error) { return len(p), nil }
func (w discardWriter) WriteHeader(int)             {}

func TestServeMuxOverHTTP1(t *testing.T) {
	mux := NewServeMux()
	mux.HandleFunc("GET /users/{id}", func(w ResponseWriter, r *Request) {
		_, _ = w.WriteString("user " + r.PathValue("id"))
	})
	mux.HandleFunc("/dir/", func(w ResponseWriter, r *Request) {})
	ts := startServer(t, mux.ServeHTTP)
	c := dialRaw(t, ts.addr)
	c.send("GET /users/42 HTTP/1.1\r\nHost: x\r\n\r\n" +
		"HEAD /users/42 HTTP/1.1\r\nHost: x\r\n\r\n" +
		"DELETE /users/42 HTTP/1.1\r\nHost: x\r\n\r\n" +
		"GET /dir?q=1 HTTP/1.1\r\nHost: x\r\n\r\n" +
		"GET /dir/../users/7 HTTP/1.1\r\nHost: x\r\n\r\n" +
		"GET /nope HTTP/1.1\r\nHost: x\r\n\r\n" +
		"OPTIONS * HTTP/1.1\r\nHost: x\r\n\r\n")
	checks := []struct {
		method string
		status int
		header string
		value  string
		body   string
	}{
		{"GET", 200, "Content-Length", "7", "user 42"},
		{"HEAD", 200, "Content-Length", "7", ""},
		{"DELETE", 405, "Allow", "GET, HEAD", "Method Not Allowed\n"},
		{"GET", 307, "Location", "/dir/?q=1", "<a href=\"/dir/?q=1\">Temporary Redirect</a>.\n\n"},
		{"GET", 307, "Location", "/users/7", "<a href=\"/users/7\">Temporary Redirect</a>.\n\n"},
		{"GET", 404, "X-Content-Type-Options", "nosniff", "404 page not found\n"},
		{"OPTIONS", 400, "Content-Length", "0", ""},
	}
	for i, want := range checks {
		resp := c.response(want.method)
		if resp.StatusCode != want.status || resp.Header.Get(want.header) != want.value || resp.body != want.body {
			t.Errorf("response %d: status %d, %s %q, body %q", i, resp.StatusCode, want.header,
				resp.Header.Get(want.header), resp.body)
		}
		if last := i == len(checks)-1; resp.Close != last {
			t.Errorf("response %d: Close = %v, want %v", i, resp.Close, last)
		}
	}
}
