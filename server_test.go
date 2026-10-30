package qilat

import (
	"bufio"
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"net"
	"net/http"
	"net/http/httptrace"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// checkGoroutines fails the test if goroutines started during it are still running at the end.
// Register it first so its cleanup runs after the server's.
func checkGoroutines(t *testing.T) {
	t.Helper()
	before := runtime.NumGoroutine()
	t.Cleanup(func() {
		deadline := time.Now().Add(3 * time.Second)
		for runtime.NumGoroutine() > before && time.Now().Before(deadline) {
			time.Sleep(5 * time.Millisecond)
		}
		if n := runtime.NumGoroutine(); n > before {
			buf := make([]byte, 1<<20)
			t.Errorf("goroutine leak: %d before, %d after\n%s", before, n, buf[:runtime.Stack(buf, true)])
		}
	})
}

type testServer struct {
	*Server
	addr string
	done chan error
	logs *syncBuffer
}

type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.String()
}

func startServer(t *testing.T, h HandlerFunc, configure ...func(*Server)) *testServer {
	t.Helper()
	checkGoroutines(t)
	logs := &syncBuffer{}
	srv := &Server{Handler: h, ErrorLog: slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug}))}
	for _, f := range configure {
		f(srv)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ts := &testServer{Server: srv, addr: ln.Addr().String(), done: make(chan error, 1), logs: logs}
	go func() { ts.done <- srv.Serve(ln) }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
		if err := <-ts.done; !errors.Is(err, ErrServerClosed) {
			t.Errorf("Serve returned %v", err)
		}
	})
	return ts
}

func (ts *testServer) url(path string) string { return "http://" + ts.addr + path }

func newClient(t *testing.T) *http.Client {
	t.Helper()
	tr := &http.Transport{}
	t.Cleanup(tr.CloseIdleConnections)
	return &http.Client{Transport: tr, Timeout: 5 * time.Second}
}

// rawConn is a raw TCP client, so tests control the exact bytes and use net/http only as a response parser.
type rawConn struct {
	t  *testing.T
	nc net.Conn
	br *bufio.Reader
}

func dialRaw(t *testing.T, addr string) *rawConn {
	t.Helper()
	nc, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = nc.Close() })
	_ = nc.SetDeadline(time.Now().Add(5 * time.Second))
	return &rawConn{t: t, nc: nc, br: bufio.NewReader(nc)}
}

func (c *rawConn) send(s string) {
	c.t.Helper()
	if _, err := io.WriteString(c.nc, s); err != nil {
		c.t.Fatal(err)
	}
}

// rawResponse is a fully read response, so tests never hold an open body.
type rawResponse struct {
	StatusCode       int
	Header           http.Header
	Close            bool
	ContentLength    int64
	TransferEncoding []string
	body             string
}

func (c *rawConn) response(method string) *rawResponse {
	c.t.Helper()
	resp, err := http.ReadResponse(c.br, &http.Request{Method: method})
	if err != nil {
		c.t.Fatalf("read response: %v", err)
	}
	b, err := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if err != nil {
		c.t.Fatalf("read body: %v", err)
	}
	return &rawResponse{resp.StatusCode, resp.Header, resp.Close, resp.ContentLength, resp.TransferEncoding, string(b)}
}

func (c *rawConn) body(resp *rawResponse) string { return resp.body }

// closed reports whether the server closed the connection.
func (c *rawConn) closed() bool {
	_ = c.nc.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, err := c.br.ReadByte()
	return err != nil && !isTimeout(err)
}

func isTimeout(err error) bool {
	var ne net.Error
	return errors.As(err, &ne) && ne.Timeout()
}

func echoHandler(w ResponseWriter, r *Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(400)
		_, _ = w.WriteString(err.Error())
		return
	}
	w.Header().Set("X-Method", r.Method())
	w.Header().Set("X-Path", string(r.Path()))
	_, _ = w.Write(body)
}

func TestServerKeepAliveWithNetHTTPClient(t *testing.T) {
	ts := startServer(t, echoHandler)
	client := newClient(t)
	reused := 0
	for i := range 3 {
		trace := &httptrace.ClientTrace{GotConn: func(info httptrace.GotConnInfo) {
			if info.Reused {
				reused++
			}
		}}
		req, _ := http.NewRequestWithContext(httptrace.WithClientTrace(context.Background(), trace),
			"POST", ts.url("/echo/"+strconv.Itoa(i)), strings.NewReader("body"))
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		got, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode != 200 || string(got) != "body" || resp.Header.Get("X-Path") != "/echo/"+strconv.Itoa(i) {
			t.Fatalf("status %d body %q path %q", resp.StatusCode, got, resp.Header.Get("X-Path"))
		}
		if resp.ContentLength != 4 || resp.Header.Get("Date") == "" {
			t.Fatalf("ContentLength %d Date %q", resp.ContentLength, resp.Header.Get("Date"))
		}
	}
	if reused != 2 {
		t.Fatalf("reused %d connections, want 2", reused)
	}
}

func TestServerChunkedUpload(t *testing.T) {
	ts := startServer(t, echoHandler)
	pr, pw := io.Pipe()
	go func() {
		for range 3 {
			_, _ = pw.Write([]byte("chunk-"))
		}
		_ = pw.Close()
	}()
	resp, err := newClient(t).Post(ts.url("/"), "text/plain", pr)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	got, _ := io.ReadAll(resp.Body)
	if string(got) != "chunk-chunk-chunk-" {
		t.Fatalf("body = %q", got)
	}
}

func TestServerExpectContinue(t *testing.T) {
	ts := startServer(t, echoHandler)
	c := dialRaw(t, ts.addr)
	c.send("POST / HTTP/1.1\r\nHost: h\r\nContent-Length: 5\r\nExpect: 100-continue\r\n\r\n")
	if resp := c.response("POST"); resp.StatusCode != 100 {
		t.Fatalf("status = %d, want 100", resp.StatusCode)
	}
	c.send("hello")
	resp := c.response("POST")
	if body := c.body(resp); resp.StatusCode != 200 || body != "hello" {
		t.Fatalf("status %d body %q", resp.StatusCode, body)
	}
	// The connection is still usable.
	c.send("GET / HTTP/1.1\r\nHost: h\r\n\r\n")
	if resp := c.response("GET"); resp.StatusCode != 200 {
		t.Fatalf("second status = %d", resp.StatusCode)
	}
}

func TestServerExpectContinueNotRead(t *testing.T) {
	ts := startServer(t, func(w ResponseWriter, r *Request) { w.WriteHeader(401) })
	c := dialRaw(t, ts.addr)
	c.send("POST / HTTP/1.1\r\nHost: h\r\nContent-Length: 5\r\nExpect: 100-continue\r\n\r\n")
	resp := c.response("POST")
	if resp.StatusCode != 401 {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	_ = c.body(resp)
	if !c.closed() {
		t.Fatal("connection with an unsent body was kept open")
	}
}

func TestServerNetHTTPExpectContinue(t *testing.T) {
	ts := startServer(t, echoHandler)
	tr := &http.Transport{ExpectContinueTimeout: 5 * time.Second}
	defer tr.CloseIdleConnections()
	req, _ := http.NewRequest("PUT", ts.url("/"), strings.NewReader("payload"))
	req.Header.Set("Expect", "100-continue")
	resp, err := (&http.Client{Transport: tr}).Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if got, _ := io.ReadAll(resp.Body); string(got) != "payload" {
		t.Fatalf("body = %q", got)
	}
}

func TestServerHEAD(t *testing.T) {
	ts := startServer(t, func(w ResponseWriter, r *Request) { _, _ = w.WriteString("hidden body") })
	c := dialRaw(t, ts.addr)
	c.send("HEAD / HTTP/1.1\r\nHost: h\r\n\r\nGET / HTTP/1.1\r\nHost: h\r\n\r\n")
	resp := c.response("HEAD")
	if resp.ContentLength != 11 {
		t.Fatalf("HEAD Content-Length = %d", resp.ContentLength)
	}
	// If the HEAD response carried a body, it would corrupt the next response.
	if resp := c.response("GET"); c.body(resp) != "hidden body" {
		t.Fatal("response after HEAD is corrupted")
	}
}

func TestServerStreamingResponse(t *testing.T) {
	ts := startServer(t, func(w ResponseWriter, r *Request) {
		for i := range 3 {
			_, _ = fmt.Fprintf(w, "part%d;", i)
			_ = w.Flush()
		}
		_, _ = w.Write(bytes.Repeat([]byte("z"), 2*responseBufferSize))
	})
	c := dialRaw(t, ts.addr)
	c.send("GET / HTTP/1.1\r\nHost: h\r\n\r\n")
	resp := c.response("GET")
	body := c.body(resp)
	if len(resp.TransferEncoding) != 1 || resp.TransferEncoding[0] != "chunked" {
		t.Fatalf("TransferEncoding = %q", resp.TransferEncoding)
	}
	if body != "part0;part1;part2;"+strings.Repeat("z", 2*responseBufferSize) {
		t.Fatalf("body length %d", len(body))
	}
}

func TestServerHTTP10(t *testing.T) {
	ts := startServer(t, func(w ResponseWriter, r *Request) {
		if r.Proto() != "HTTP/1.0" {
			t.Errorf("Proto = %q", r.Proto())
		}
		_, _ = w.WriteString("ok")
		if r.URI().Query().Has("stream") {
			_ = w.Flush()
		}
	})
	c := dialRaw(t, ts.addr)
	c.send("GET / HTTP/1.0\r\n\r\n")
	if resp := c.response("GET"); c.body(resp) != "ok" || !resp.Close {
		t.Fatalf("Close = %v", resp.Close)
	}
	if !c.closed() {
		t.Fatal("HTTP/1.0 connection without keep-alive stayed open")
	}

	c = dialRaw(t, ts.addr)
	c.send("GET / HTTP/1.0\r\nConnection: keep-alive\r\n\r\n")
	if resp := c.response("GET"); resp.Header.Get("Connection") != "keep-alive" || c.body(resp) != "ok" {
		t.Fatalf("Connection = %q", resp.Header.Get("Connection"))
	}
	// A streamed body has no length on HTTP/1.0, so the server must close to end it.
	c.send("GET /?stream HTTP/1.0\r\nConnection: keep-alive\r\n\r\n")
	if resp := c.response("GET"); c.body(resp) != "ok" || !resp.Close {
		t.Fatal("streamed HTTP/1.0 response was not close-delimited")
	}
}

func TestServerPipelining(t *testing.T) {
	ts := startServer(t, echoHandler)
	c := dialRaw(t, ts.addr)
	c.send("GET /1 HTTP/1.1\r\nHost: h\r\n\r\n" +
		"POST /2 HTTP/1.1\r\nHost: h\r\nContent-Length: 3\r\n\r\nabc" +
		"POST /3 HTTP/1.1\r\nHost: h\r\nTransfer-Encoding: chunked\r\n\r\n2\r\nxy\r\n0\r\n\r\n" +
		"GET /4 HTTP/1.1\r\nHost: h\r\nConnection: close\r\n\r\n")
	for i, want := range []string{"", "abc", "xy", ""} {
		method := "GET"
		if i == 1 || i == 2 {
			method = "POST"
		}
		resp := c.response(method)
		if got := c.body(resp); got != want || resp.Header.Get("X-Path") != "/"+strconv.Itoa(i+1) {
			t.Fatalf("response %d: body %q path %q", i+1, got, resp.Header.Get("X-Path"))
		}
	}
	if !c.closed() {
		t.Fatal("connection not closed after Connection: close")
	}
}

func TestServerRejectsMalformed(t *testing.T) {
	ts := startServer(t, echoHandler, func(s *Server) { s.MaxHeaderBytes = 1024 })
	tests := []struct {
		name, raw string
		status    int
	}{
		{"CL and TE", "POST / HTTP/1.1\r\nHost: h\r\nContent-Length: 4\r\nTransfer-Encoding: chunked\r\n\r\n", 400},
		{"duplicate CL", "POST / HTTP/1.1\r\nHost: h\r\nContent-Length: 4\r\nContent-Length: 4\r\n\r\n", 400},
		{"TE gzip", "POST / HTTP/1.1\r\nHost: h\r\nTransfer-Encoding: gzip\r\n\r\n", 501},
		{"bare LF", "GET / HTTP/1.1\nHost: h\n\n", 400},
		{"obs-fold", "GET / HTTP/1.1\r\nHost: h\r\nX: a\r\n b\r\n\r\n", 400},
		{"missing Host", "GET / HTTP/1.1\r\n\r\n", 400},
		{"version", "GET / HTTP/1.2\r\nHost: h\r\n\r\n", 505},
		{"expectation", "GET / HTTP/1.1\r\nHost: h\r\nExpect: nope\r\n\r\n", 417},
		{"header too large", "GET / HTTP/1.1\r\nHost: h\r\nX: " + strings.Repeat("a", 2000) + "\r\n\r\n", 431},
		{"invalid target", "GET /a%zz HTTP/1.1\r\nHost: h\r\n\r\n", 400},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := dialRaw(t, ts.addr)
			c.send(tt.raw)
			resp := c.response("GET")
			body := c.body(resp)
			if resp.StatusCode != tt.status || !resp.Close || !strings.HasPrefix(body, strconv.Itoa(tt.status)) {
				t.Fatalf("status %d close %v body %q, want %d", resp.StatusCode, resp.Close, body, tt.status)
			}
			if !c.closed() {
				t.Fatal("connection stayed open after a rejected request")
			}
		})
	}
}

func TestServerBadChunkedBody(t *testing.T) {
	ts := startServer(t, echoHandler)
	c := dialRaw(t, ts.addr)
	c.send("POST / HTTP/1.1\r\nHost: h\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhelloXX")
	resp := c.response("POST")
	if body := c.body(resp); resp.StatusCode != 400 || !strings.Contains(body, "CRLF") {
		t.Fatalf("status %d body %q", resp.StatusCode, body)
	}
	if !c.closed() {
		t.Fatal("connection stayed open after a malformed body")
	}
}

func TestServerBodyLimit(t *testing.T) {
	var gotErr error
	ts := startServer(t, func(w ResponseWriter, r *Request) {
		_, gotErr = io.ReadAll(r.Body)
	}, func(s *Server) { s.MaxRequestBodySize = 8 })

	c := dialRaw(t, ts.addr)
	c.send("POST / HTTP/1.1\r\nHost: h\r\nContent-Length: 9\r\n\r\n123456789")
	if resp := c.response("POST"); resp.StatusCode != 413 {
		t.Fatalf("status = %d, want 413", resp.StatusCode)
	}

	c = dialRaw(t, ts.addr)
	c.send("POST / HTTP/1.1\r\nHost: h\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhello\r\n5\r\nworld\r\n0\r\n\r\n")
	resp := c.response("POST")
	_ = c.body(resp)
	if !errors.Is(gotErr, ErrBodyTooLarge) {
		t.Fatalf("handler read error = %v", gotErr)
	}
	if !c.closed() {
		t.Fatal("connection stayed open after an oversized body")
	}
}

func TestServerUnreadBody(t *testing.T) {
	ts := startServer(t, func(w ResponseWriter, r *Request) { _, _ = w.WriteString("ignored body") })
	c := dialRaw(t, ts.addr)
	c.send("POST / HTTP/1.1\r\nHost: h\r\nContent-Length: 5\r\n\r\nhelloGET / HTTP/1.1\r\nHost: h\r\n\r\n")
	for range 2 {
		if resp := c.response("POST"); c.body(resp) != "ignored body" {
			t.Fatal("unread body was not drained")
		}
	}
	big := strconv.Itoa(maxDrainForTest + 1)
	c.send("POST / HTTP/1.1\r\nHost: h\r\nContent-Length: " + big + "\r\n\r\n")
	if resp := c.response("POST"); c.body(resp) != "ignored body" {
		t.Fatal("unexpected response")
	}
	if !c.closed() {
		t.Fatal("connection kept alive with a large unread body")
	}
}

// maxDrainForTest mirrors h1.maxDrain.
const maxDrainForTest = 256 << 10

func TestServerHeaderTimeout(t *testing.T) {
	ts := startServer(t, echoHandler, func(s *Server) { s.ReadHeaderTimeout = 100 * time.Millisecond })
	c := dialRaw(t, ts.addr)
	c.send("GET / HTTP/1.1\r\nHost: h\r\n")
	if !c.closed() {
		t.Fatal("slow header was not timed out")
	}
	c = dialRaw(t, ts.addr)
	if !c.closed() {
		t.Fatal("silent connection was not timed out")
	}
}

func TestServerIdleTimeout(t *testing.T) {
	ts := startServer(t, echoHandler, func(s *Server) { s.IdleTimeout = 100 * time.Millisecond })
	c := dialRaw(t, ts.addr)
	c.send("GET / HTTP/1.1\r\nHost: h\r\n\r\n")
	_ = c.body(c.response("GET"))
	if !c.closed() {
		t.Fatal("idle connection was not closed")
	}
}

func TestServerShutdownGraceful(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	ts := startServer(t, func(w ResponseWriter, r *Request) {
		if r.URI().Query().Has("slow") {
			close(started)
			<-release
		}
		_, _ = w.WriteString("done")
	})
	idle := dialRaw(t, ts.addr)
	idle.send("GET / HTTP/1.1\r\nHost: h\r\n\r\n")
	_ = idle.body(idle.response("GET"))

	active := dialRaw(t, ts.addr)
	active.send("GET /?slow HTTP/1.1\r\nHost: h\r\n\r\n")
	<-started

	shutdown := make(chan error, 1)
	go func() { shutdown <- ts.Shutdown(context.Background()) }()
	if !idle.closed() {
		t.Fatal("idle connection not closed by Shutdown")
	}
	select {
	case err := <-shutdown:
		t.Fatalf("Shutdown returned %v before the active request finished", err)
	case <-time.After(50 * time.Millisecond):
	}
	if _, err := net.Dial("tcp", ts.addr); err == nil {
		t.Fatal("listener still accepting during Shutdown")
	}
	close(release)
	resp := active.response("GET")
	if body := active.body(resp); body != "done" || !resp.Close {
		t.Fatalf("body %q close %v", body, resp.Close)
	}
	if err := <-shutdown; err != nil {
		t.Fatalf("Shutdown = %v", err)
	}
	if err := ts.ListenAndServe("127.0.0.1:0"); !errors.Is(err, ErrServerClosed) {
		t.Fatalf("ListenAndServe after Shutdown = %v", err)
	}
}

func TestServerShutdownDeadlineCancelsRequests(t *testing.T) {
	started := make(chan struct{})
	ctxErr := make(chan error, 1)
	ts := startServer(t, func(w ResponseWriter, r *Request) {
		close(started)
		<-r.Context().Done()
		ctxErr <- r.Context().Err()
	})
	c := dialRaw(t, ts.addr)
	c.send("GET / HTTP/1.1\r\nHost: h\r\n\r\n")
	<-started
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := ts.Shutdown(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Shutdown = %v", err)
	}
	if err := <-ctxErr; !errors.Is(err, context.Canceled) {
		t.Fatalf("request context error = %v", err)
	}
}

func TestServerClose(t *testing.T) {
	started := make(chan struct{})
	ts := startServer(t, func(w ResponseWriter, r *Request) {
		close(started)
		<-r.Context().Done()
	})
	c := dialRaw(t, ts.addr)
	c.send("GET / HTTP/1.1\r\nHost: h\r\n\r\n")
	<-started
	if err := ts.Close(); err != nil {
		t.Fatal(err)
	}
	if !c.closed() {
		t.Fatal("Close left the connection open")
	}
}

func TestServerClientDisconnectCancelsContext(t *testing.T) {
	started := make(chan struct{}, 1)
	ctxErr := make(chan error, 1)
	ts := startServer(t, func(w ResponseWriter, r *Request) {
		// A disconnect is only observable once the body is consumed, as in net/http.
		_, _ = io.ReadAll(r.Body)
		started <- struct{}{}
		select {
		case <-r.Context().Done():
			ctxErr <- r.Context().Err()
		case <-time.After(3 * time.Second):
			ctxErr <- errors.New("context not canceled")
		}
	})
	for _, raw := range []string{
		"GET / HTTP/1.1\r\nHost: h\r\n\r\n",
		"POST / HTTP/1.1\r\nHost: h\r\nContent-Length: 2\r\n\r\nhi",
		"POST / HTTP/1.1\r\nHost: h\r\nTransfer-Encoding: chunked\r\n\r\n2\r\nhi\r\n0\r\n\r\n",
	} {
		c := dialRaw(t, ts.addr)
		c.send(raw)
		<-started
		_ = c.nc.Close()
		if err := <-ctxErr; !errors.Is(err, context.Canceled) {
			t.Fatalf("%q: context error = %v", raw, err)
		}
	}
}

func TestServerDisconnectWatchKeepsPipelinedBytes(t *testing.T) {
	ts := startServer(t, func(w ResponseWriter, r *Request) {
		// Waiting on Done starts the background read, which must hand over bytes of the next request.
		select {
		case <-r.Context().Done():
		case <-time.After(100 * time.Millisecond):
		}
		_, _ = w.WriteString(string(r.Path()))
	})
	c := dialRaw(t, ts.addr)
	c.send("GET /first HTTP/1.1\r\nHost: h\r\n\r\n")
	time.Sleep(20 * time.Millisecond)
	c.send("GET /second HTTP/1.1\r\nHost: h\r\n\r\n")
	for _, want := range []string{"/first", "/second"} {
		if got := c.body(c.response("GET")); got != want {
			t.Fatalf("body = %q, want %q", got, want)
		}
	}
}

func TestServerHijack(t *testing.T) {
	ts := startServer(t, func(w ResponseWriter, r *Request) {
		conn, rw, err := w.(Hijacker).Hijack()
		if err != nil {
			t.Error(err)
			return
		}
		defer func() { _ = conn.Close() }()
		if _, _, err := w.(Hijacker).Hijack(); !errors.Is(err, ErrHijacked) {
			t.Errorf("second Hijack = %v", err)
		}
		if _, err := w.Write([]byte("x")); !errors.Is(err, ErrHijacked) {
			t.Errorf("Write after Hijack = %v", err)
		}
		if err := w.Flush(); !errors.Is(err, ErrHijacked) {
			t.Errorf("Flush after Hijack = %v", err)
		}
		line, _ := rw.ReadString('\n')
		_, _ = rw.WriteString("echo:" + line)
		_ = rw.Flush()
	})
	c := dialRaw(t, ts.addr)
	c.send("GET / HTTP/1.1\r\nHost: h\r\n\r\nping\n")
	got, err := c.br.ReadString('\n')
	if err != nil || got != "echo:ping\n" {
		t.Fatalf("hijacked conn returned %q, %v", got, err)
	}
}

func TestServerEarlyHints(t *testing.T) {
	ts := startServer(t, func(w ResponseWriter, r *Request) {
		w.Header().Set("Link", "</style.css>; rel=preload")
		w.WriteHeader(103)
		w.Header().Set("Content-Type", "text/html")
		_, _ = w.WriteString("page")
	})
	c := dialRaw(t, ts.addr)
	c.send("GET / HTTP/1.1\r\nHost: h\r\n\r\n")
	hints := c.response("GET")
	if hints.StatusCode != 103 || hints.Header.Get("Link") == "" {
		t.Fatalf("first response %d %v", hints.StatusCode, hints.Header)
	}
	final := c.response("GET")
	if final.StatusCode != 200 || c.body(final) != "page" || final.Header.Get("Link") == "" {
		t.Fatalf("final response %d %v", final.StatusCode, final.Header)
	}

	c = dialRaw(t, ts.addr)
	c.send("GET / HTTP/1.0\r\n\r\n")
	if resp := c.response("GET"); resp.StatusCode != 200 {
		t.Fatalf("HTTP/1.0 client got status %d, want the 103 to be skipped", resp.StatusCode)
	}
}

func TestServerPanic(t *testing.T) {
	ts := startServer(t, func(w ResponseWriter, r *Request) {
		switch string(r.Path()) {
		case "/panic":
			panic("boom")
		case "/abort":
			panic(ErrAbortHandler)
		}
		_, _ = w.WriteString("alive")
	})
	for _, path := range []string{"/panic", "/abort"} {
		c := dialRaw(t, ts.addr)
		c.send("GET " + path + " HTTP/1.1\r\nHost: h\r\n\r\n")
		if !c.closed() {
			t.Fatalf("%s: connection not closed after panic", path)
		}
	}
	c := dialRaw(t, ts.addr)
	c.send("GET / HTTP/1.1\r\nHost: h\r\n\r\n")
	if body := c.body(c.response("GET")); body != "alive" {
		t.Fatalf("server not serving after panic: %q", body)
	}
	logs := ts.logs.String()
	if !strings.Contains(logs, "panic serving request") || !strings.Contains(logs, "boom") {
		t.Fatalf("panic not logged: %s", logs)
	}
	if strings.Count(logs, "panic serving request") != 1 {
		t.Fatalf("ErrAbortHandler was logged: %s", logs)
	}
}

func TestServerMaxConcurrentConns(t *testing.T) {
	ts := startServer(t, func(w ResponseWriter, r *Request) { _, _ = w.WriteString("ok") },
		func(s *Server) { s.MaxConcurrentConns = 1 })
	first := dialRaw(t, ts.addr)
	first.send("GET / HTTP/1.1\r\nHost: h\r\n\r\n")
	_ = first.body(first.response("GET"))

	second := dialRaw(t, ts.addr)
	second.send("GET / HTTP/1.1\r\nHost: h\r\n\r\n")
	_ = second.nc.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
	if _, err := second.br.ReadByte(); !isTimeout(err) {
		t.Fatalf("second connection served while the limit was reached: %v", err)
	}
	_ = first.nc.Close()
	_ = second.nc.SetReadDeadline(time.Now().Add(2 * time.Second))
	if body := second.body(second.response("GET")); body != "ok" {
		t.Fatalf("second connection body %q", body)
	}
}

func TestServerTLS(t *testing.T) {
	checkGoroutines(t)
	cert := selfSignedCert(t)
	var sawTLS bool
	srv := &Server{
		Handler: HandlerFunc(func(w ResponseWriter, r *Request) {
			sawTLS = r.TLS() != nil && r.TLS().HandshakeComplete
			_, _ = w.WriteString("secure")
		}),
		TLSConfig: &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12},
		ErrorLog:  slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- srv.ServeTLS(ln, "", "") }()
	tr := &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}} //nolint:gosec // self-signed test certificate
	defer tr.CloseIdleConnections()
	resp, err := (&http.Client{Transport: tr}).Get("https://" + ln.Addr().String() + "/")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if string(body) != "secure" || !sawTLS {
		t.Fatalf("body %q sawTLS %v", body, sawTLS)
	}
	// A plain-text client must fail the handshake without disturbing the server.
	c := dialRaw(t, ln.Addr().String())
	c.send("GET / HTTP/1.1\r\nHost: h\r\n\r\n")
	_ = c.closed()
	tr.CloseIdleConnections()
	if err := srv.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := <-done; !errors.Is(err, ErrServerClosed) {
		t.Fatalf("ServeTLS = %v", err)
	}
	if err := (&Server{Handler: srv.Handler}).ServeTLS(listenLocal(t), "missing.pem", "missing.key"); err == nil {
		t.Fatal("ServeTLS with missing files succeeded")
	}
}

func TestServerReduceMemoryUsage(t *testing.T) {
	ts := startServer(t, echoHandler, func(s *Server) { s.ReduceMemoryUsage = true })
	c := dialRaw(t, ts.addr)
	for i := range 3 {
		c.send("POST /" + strconv.Itoa(i) + " HTTP/1.1\r\nHost: h\r\nContent-Length: 2\r\n\r\nok")
		if body := c.body(c.response("POST")); body != "ok" {
			t.Fatalf("body = %q", body)
		}
	}
	c.send("GET /a HTTP/1.1\r\nHost: h\r\n\r\nGET /b HTTP/1.1\r\nHost: h\r\n\r\n")
	for _, want := range []string{"/a", "/b"} {
		if resp := c.response("GET"); resp.Header.Get("X-Path") != want {
			t.Fatalf("path = %q, want %q", resp.Header.Get("X-Path"), want)
		}
	}
}

func TestServerDebugPoison(t *testing.T) {
	if raceEnabled {
		// Using a request from another goroutine after release is itself a data race, which -race reports.
		t.Skip("the race detector reports this misuse before poisoning can")
	}
	kept := make(chan *Request, 1)
	ts := startServer(t, func(w ResponseWriter, r *Request) { kept <- r }, func(s *Server) { s.DebugPoison = true })
	c := dialRaw(t, ts.addr)
	c.send("GET / HTTP/1.1\r\nHost: h\r\n\r\n")
	_ = c.body(c.response("GET"))
	r := <-kept
	time.Sleep(10 * time.Millisecond) // the exchange is released right after the response is flushed
	defer func() {
		if recover() == nil {
			t.Fatal("request used after ServeHTTP did not panic")
		}
	}()
	_ = r.Method()
}

func TestServerConfigErrors(t *testing.T) {
	if err := (&Server{}).Serve(listenLocal(t)); err == nil || !strings.Contains(err.Error(), "Handler is nil") {
		t.Fatalf("Serve with nil Handler = %v", err)
	}
	srv := &Server{Handler: HandlerFunc(echoHandler)}
	if err := srv.ListenAndServe("256.0.0.1:bad"); err == nil {
		t.Fatal("ListenAndServe with a bad address succeeded")
	}
	if err := srv.ListenAndServeTLS("256.0.0.1:bad", "", ""); err == nil {
		t.Fatal("ListenAndServeTLS with a bad address succeeded")
	}
	_ = srv.Close()
	if err := srv.Serve(listenLocal(t)); !errors.Is(err, ErrServerClosed) {
		t.Fatalf("Serve after Close = %v", err)
	}
	if err := srv.ListenAndServeTLS("", "", ""); !errors.Is(err, ErrServerClosed) {
		t.Fatalf("ListenAndServeTLS after Close = %v", err)
	}
	defer func() {
		if recover() == nil {
			t.Fatal("nil BaseContext result did not panic")
		}
	}()
	_ = (&Server{Handler: srv.Handler, BaseContext: func(net.Listener) context.Context { return nil }}).Serve(listenLocal(t))
}

func TestServerBaseContextAndLimitsResolution(t *testing.T) {
	type key struct{}
	got := make(chan any, 1)
	ts := startServer(t, func(w ResponseWriter, r *Request) { got <- r.Context().Value(key{}) },
		func(s *Server) {
			s.BaseContext = func(net.Listener) context.Context { return context.WithValue(context.Background(), key{}, "base") }
			s.MaxHeaderBytes, s.MaxRequestBodySize = -1, -1
			s.ReadHeaderTimeout, s.IdleTimeout, s.ReadTimeout, s.WriteTimeout = -1, -1, time.Minute, time.Minute
		})
	c := dialRaw(t, ts.addr)
	c.send("GET / HTTP/1.1\r\nHost: h\r\n\r\n")
	_ = c.body(c.response("GET"))
	if v := <-got; v != "base" {
		t.Fatalf("context value = %v", v)
	}
	if timeoutOr(0, time.Second) != time.Second || timeoutOr(-1, time.Second) != 0 || timeoutOr(2, time.Second) != 2 {
		t.Fatal("timeoutOr resolution")
	}
}

func TestServerAllocs(t *testing.T) {
	if raceEnabled {
		t.Skip("allocation counts are not meaningful with the race detector")
	}
	body := []byte("Hello, World!")
	ts := startServer(t, func(w ResponseWriter, r *Request) {
		w.Header().Set("Content-Type", "text/plain")
		_, _ = w.Write(body)
	})
	c := dialRaw(t, ts.addr)
	req := []byte("GET /plaintext HTTP/1.1\r\nHost: bench\r\n\r\n")
	buf := make([]byte, 4096)
	roundTrip := func() {
		if _, err := c.nc.Write(req); err != nil {
			t.Fatal(err)
		}
		// The response ends with the 13-byte body.
		var got []byte
		for !bytes.HasSuffix(got, body) {
			n, err := c.nc.Read(buf)
			if err != nil {
				t.Fatal(err)
			}
			got = append(buf[:0], buf[:n]...)
		}
	}
	roundTrip()
	if n := testing.AllocsPerRun(200, roundTrip); n != 0 {
		t.Fatalf("allocs per keep-alive request = %v, want 0", n)
	}
}

func listenLocal(t *testing.T) net.Listener {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	return ln
}

func selfSignedCert(t *testing.T) tls.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "localhost"},
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
}

// TestReadTimeoutKeepsContext checks that ReadTimeout bounds reading the request, not the handler: the
// disconnect watcher must not take the expired read deadline for a closed connection.
func TestReadTimeoutKeepsContext(t *testing.T) {
	ts := startServer(t, func(w ResponseWriter, r *Request) {
		select {
		case <-r.Context().Done():
			_, _ = io.WriteString(w, "canceled")
		case <-time.After(500 * time.Millisecond):
			_, _ = io.WriteString(w, "ok")
		}
	}, func(s *Server) { s.ReadTimeout = 100 * time.Millisecond })
	resp, err := newClient(t).Get(ts.url("/"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if b, _ := io.ReadAll(resp.Body); string(b) != "ok" {
		t.Fatalf("handler saw %q", b)
	}
}
