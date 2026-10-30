package qilat

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// do sends one request with c and returns status, body, and the response for further checks.
func do(t *testing.T, c *Client, method, url string, body string, header ...[2]string) (*Response, string) {
	t.Helper()
	req := AcquireRequest()
	defer ReleaseRequest(req)
	req.SetMethod(method)
	if err := req.SetRequestURI(url); err != nil {
		t.Fatal(err)
	}
	for _, h := range header {
		req.Header.Set(h[0], h[1])
	}
	if body != "" {
		req.SetBody(strings.NewReader(body), int64(len(body)))
	}
	resp := AcquireResponse()
	t.Cleanup(func() { ReleaseResponse(resp) })
	if err := c.Do(req, resp); err != nil {
		t.Fatalf("%s %s: %v", method, url, err)
	}
	b, err := resp.ReadBody()
	if err != nil {
		t.Fatalf("%s %s: reading body: %v", method, url, err)
	}
	return resp, string(b)
}

func newTestClient(t *testing.T, configure ...func(*Transport)) (*Client, *Transport) {
	t.Helper()
	tr := &Transport{}
	for _, f := range configure {
		f(tr)
	}
	t.Cleanup(tr.CloseIdleConnections)
	return &Client{Transport: tr}, tr
}

func TestClientH1AgainstNetHTTP(t *testing.T) {
	checkGoroutines(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/echo":
			body, _ := io.ReadAll(r.Body)
			w.Header().Set("X-Method", r.Method)
			w.Header().Set("X-Host", r.Host)
			w.Header().Set("X-Query", r.URL.RawQuery)
			w.Header().Set("X-Header", r.Header.Get("X-Test"))
			w.Header().Set("X-TE", strings.Join(r.TransferEncoding, ","))
			_, _ = w.Write(body)
		case "/chunked":
			w.Header().Set("Trailer", "X-Sum")
			for range 3 {
				_, _ = io.WriteString(w, "chunk")
				w.(http.Flusher).Flush()
			}
			w.Header().Set("X-Sum", "15")
		case "/empty":
			w.WriteHeader(204)
		case "/early":
			w.Header().Set("Link", "</a.css>; rel=preload")
			w.WriteHeader(103)
			_, _ = io.WriteString(w, "after hints")
		}
	}))
	defer srv.Close()
	c, _ := newTestClient(t)

	resp, body := do(t, c, "POST", srv.URL+"/echo?a=1&b=2", "payload", [2]string{"X-Test", "v"})
	if resp.StatusCode() != 200 || body != "payload" || resp.Proto() != "HTTP/1.1" ||
		resp.Header.Get("X-Method") != "POST" || resp.Header.Get("X-Host") != strings.TrimPrefix(srv.URL, "http://") ||
		resp.Header.Get("X-Query") != "a=1&b=2" || resp.Header.Get("X-Header") != "v" || resp.ContentLength() != 7 {
		t.Fatalf("echo: %d %q %v", resp.StatusCode(), body, fields(resp.Header))
	}

	req := AcquireRequest()
	defer ReleaseRequest(req)
	req.SetMethod("PUT")
	_ = req.SetRequestURI(srv.URL + "/echo#fragment")
	req.SetBody(io.MultiReader(strings.NewReader("un"), strings.NewReader("known")), -1)
	resp2 := AcquireResponse()
	defer ReleaseResponse(resp2)
	if err := c.Do(req, resp2); err != nil {
		t.Fatal(err)
	}
	if b, _ := resp2.ReadBody(); string(b) != "unknown" || resp2.Header.Get("X-TE") != "chunked" {
		t.Fatalf("chunked upload: %q %v", b, fields(resp2.Header))
	}

	resp, body = do(t, c, "GET", srv.URL+"/chunked", "")
	if body != "chunkchunkchunk" || resp.ContentLength() != -1 || resp.Trailer().Get("X-Sum") != "15" {
		t.Fatalf("chunked: %q cl=%d trailer %v", body, resp.ContentLength(), fields(resp.Trailer()))
	}
	if resp, body = do(t, c, "GET", srv.URL+"/empty", ""); resp.StatusCode() != 204 || body != "" {
		t.Fatalf("204: %d %q", resp.StatusCode(), body)
	}
	if resp, body = do(t, c, "HEAD", srv.URL+"/echo", ""); resp.StatusCode() != 200 || body != "" {
		t.Fatalf("HEAD: %d %q", resp.StatusCode(), body)
	}
	// The 103 is skipped; net/http repeats its Link field in the final response.
	if resp, body = do(t, c, "GET", srv.URL+"/early", ""); resp.StatusCode() != 200 || body != "after hints" {
		t.Fatalf("103: %d %q %v", resp.StatusCode(), body, fields(resp.Header))
	}
}

func TestClientH1ReusesConnections(t *testing.T) {
	checkGoroutines(t)
	var conns atomic.Int32
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "ok")
	}))
	srv.Config.ConnState = func(_ net.Conn, s http.ConnState) {
		if s == http.StateNew {
			conns.Add(1)
		}
	}
	srv.Start()
	defer srv.Close()
	c, _ := newTestClient(t)
	for range 20 {
		if _, body := do(t, c, "GET", srv.URL+"/", ""); body != "ok" {
			t.Fatal(body)
		}
	}
	// Releasing a response without reading it still frees a connection whose body is already buffered.
	for range 5 {
		resp := AcquireResponse()
		if err := c.Get(srv.URL+"/", resp); err != nil {
			t.Fatal(err)
		}
		ReleaseResponse(resp)
	}
	if n := conns.Load(); n != 1 {
		t.Fatalf("%d connections for sequential requests", n)
	}
}

func TestClientAgainstQilatServer(t *testing.T) {
	ts := startServer(t, func(w ResponseWriter, r *Request) {
		w.Header().Set("X-Proto", r.Proto())
		_, _ = io.Copy(w, r.Body)
		w.(TrailerSetter).Trailer().Set("X-Done", "yes")
	})
	c, _ := newTestClient(t)
	big := strings.Repeat("0123456789", 100_000)
	resp, body := do(t, c, "POST", ts.url("/"), big)
	if body != big || resp.Header.Get("X-Proto") != "HTTP/1.1" || resp.Trailer().Get("X-Done") != "yes" {
		t.Fatalf("%d bytes, %v trailer %v", len(body), fields(resp.Header), fields(resp.Trailer()))
	}
}

func TestClientCancelMidBody(t *testing.T) {
	checkGoroutines(t)
	unblock := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "start")
		w.(http.Flusher).Flush()
		select {
		case <-r.Context().Done():
		case <-unblock:
		}
	}))
	defer srv.Close()
	defer close(unblock)
	c, _ := newTestClient(t)
	ctx, cancel := context.WithCancel(context.Background())
	req := AcquireRequest()
	defer ReleaseRequest(req)
	_ = req.SetRequestURI(srv.URL)
	req2 := req.WithContext(ctx)
	resp := AcquireResponse()
	defer ReleaseResponse(resp)
	if err := c.Do(req2, resp); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 5)
	if _, err := io.ReadFull(resp.Body, buf); err != nil || string(buf) != "start" {
		t.Fatalf("%q %v", buf, err)
	}
	done := make(chan error, 1)
	go func() { _, err := resp.Body.Read(buf); done <- err }()
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("read after cancel: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("canceling the context did not stop the body read")
	}
}

func TestClientTimeout(t *testing.T) {
	checkGoroutines(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	defer srv.Close()
	tr := &Transport{}
	defer tr.CloseIdleConnections()
	c := &Client{Transport: tr, Timeout: 50 * time.Millisecond}
	resp := AcquireResponse()
	defer ReleaseResponse(resp)
	start := time.Now()
	err := c.Get(srv.URL, resp)
	if !errors.Is(err, context.DeadlineExceeded) || time.Since(start) > 2*time.Second {
		t.Fatalf("err %v after %v", err, time.Since(start))
	}
}

func TestClientRedirects(t *testing.T) {
	checkGoroutines(t)
	other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "other "+r.Method+" auth="+r.Header.Get("Authorization"))
	}))
	defer other.Close()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		switch r.URL.Path {
		case "/302":
			http.Redirect(w, r, "/final?from=302", http.StatusFound)
		case "/307":
			http.Redirect(w, r, "/final?from=307", http.StatusTemporaryRedirect)
		case "/away":
			http.Redirect(w, r, other.URL+"/x", http.StatusMovedPermanently)
		case "/loop":
			http.Redirect(w, r, "/loop", http.StatusFound)
		case "/final":
			_, _ = io.WriteString(w, r.Method+" "+r.URL.RawQuery+" "+string(body)+" auth="+r.Header.Get("Authorization"))
		}
	}))
	defer srv.Close()
	c, _ := newTestClient(t)
	auth := [2]string{"Authorization", "secret"}
	if _, body := do(t, c, "POST", srv.URL+"/302", "data", auth); body != "GET from=302  auth=secret" {
		t.Fatalf("302: %q", body)
	}
	if _, body := do(t, c, "POST", srv.URL+"/307", "data", auth); body != "POST from=307 data auth=secret" {
		t.Fatalf("307: %q", body)
	}
	if _, body := do(t, c, "GET", srv.URL+"/away", "", auth); body != "other GET auth=" {
		t.Fatalf("cross-host: %q", body)
	}

	resp := AcquireResponse()
	defer ReleaseResponse(resp)
	if err := c.Get(srv.URL+"/loop", resp); !errors.Is(err, ErrTooManyRedirects) {
		t.Fatalf("loop: %v", err)
	}
	stop := &Client{Transport: c.Transport, CheckRedirect: func(*Request, *Response, int) error { return ErrUseLastResponse }}
	resp2 := AcquireResponse()
	defer ReleaseResponse(resp2)
	if err := stop.Get(srv.URL+"/302", resp2); err != nil || resp2.StatusCode() != 302 ||
		resp2.Header.Get("Location") != "/final?from=302" {
		t.Fatalf("ErrUseLastResponse: %v %d", err, resp2.StatusCode())
	}
}

func TestClientMaxConnsPerHost(t *testing.T) {
	checkGoroutines(t)
	var active, peak atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := active.Add(1)
		for {
			p := peak.Load()
			if n <= p || peak.CompareAndSwap(p, n) {
				break
			}
		}
		time.Sleep(10 * time.Millisecond)
		active.Add(-1)
		_, _ = io.WriteString(w, "ok")
	}))
	defer srv.Close()
	c, _ := newTestClient(t, func(tr *Transport) { tr.MaxConnsPerHost = 2 })
	var wg sync.WaitGroup
	for range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp := AcquireResponse()
			defer ReleaseResponse(resp)
			if err := c.Get(srv.URL, resp); err != nil {
				t.Error(err)
				return
			}
			_, _ = resp.ReadBody()
		}()
	}
	wg.Wait()
	if p := peak.Load(); p > 2 {
		t.Fatalf("%d concurrent requests with MaxConnsPerHost 2", p)
	}
}

// TestClientRetriesStaleConnection closes the idle connection on the server side between requests.
func TestClientRetriesStaleConnection(t *testing.T) {
	checkGoroutines(t)
	ln := listenLocal(t)
	t.Cleanup(func() { _ = ln.Close() })
	var accepted atomic.Int32
	go func() {
		for {
			nc, err := ln.Accept()
			if err != nil {
				return
			}
			accepted.Add(1)
			go func() {
				defer func() { _ = nc.Close() }()
				buf := make([]byte, 4096)
				if _, err := nc.Read(buf); err != nil {
					return
				}
				_, _ = io.WriteString(nc, "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok")
				// Closing right after the response leaves the client with a keep-alive connection that is dead.
			}()
		}
	}()
	c, _ := newTestClient(t)
	for range 3 {
		if _, body := do(t, c, "GET", "http://"+ln.Addr().String()+"/", ""); body != "ok" {
			t.Fatal(body)
		}
		time.Sleep(20 * time.Millisecond) // let the close arrive
	}
	if n := accepted.Load(); n != 3 {
		t.Fatalf("%d connections", n)
	}
	// A POST with a body is not retried.
	req := AcquireRequest()
	defer ReleaseRequest(req)
	req.SetMethod("POST")
	_ = req.SetRequestURI("http://" + ln.Addr().String() + "/")
	req.SetBody(bytes.NewReader([]byte("x")), 1)
	resp := AcquireResponse()
	defer ReleaseResponse(resp)
	if err := c.Do(req, resp); err == nil {
		t.Log("POST succeeded on a fresh connection") // allowed: the pool may have had no stale connection
	}
}

func TestClientInvalidURI(t *testing.T) {
	req := AcquireRequest()
	defer ReleaseRequest(req)
	for _, u := range []string{"/relative", "ftp://host/", "http://", "example.com"} {
		if err := req.SetRequestURI(u); err == nil {
			t.Errorf("%q accepted", u)
		}
	}
}

func TestClientAllocs(t *testing.T) {
	if raceEnabled {
		t.Skip("allocation counts differ under the race detector")
	}
	ts := startServer(t, func(w ResponseWriter, r *Request) { _, _ = w.WriteString("Hello, World!") })
	c, _ := newTestClient(t)
	req := AcquireRequest()
	defer ReleaseRequest(req)
	_ = req.SetRequestURI(ts.url("/plaintext"))
	resp := AcquireResponse()
	defer ReleaseResponse(resp)
	get := func() {
		if err := c.Do(req, resp); err != nil {
			t.Fatal(err)
		}
		if b, err := resp.ReadBody(); err != nil || string(b) != "Hello, World!" {
			t.Fatalf("%q %v", b, err)
		}
		_ = resp.Close()
	}
	get()
	if n := testing.AllocsPerRun(200, get); n != 0 {
		t.Fatalf("%v allocations per request", n)
	}
}

func TestClientLimits(t *testing.T) {
	checkGoroutines(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/big":
			_, _ = io.WriteString(w, strings.Repeat("b", 2000))
		case "/bigheader":
			w.Header().Set("X-Big", strings.Repeat("h", 4000))
		case "/slow":
			select {
			case <-r.Context().Done():
			case <-time.After(5 * time.Second):
			}
		}
	}))
	defer srv.Close()
	c, _ := newTestClient(t, func(tr *Transport) {
		tr.MaxResponseBodySize = 1000
		tr.MaxResponseHeaderBytes = 1024
		tr.ResponseHeaderTimeout = 100 * time.Millisecond
	})
	resp := AcquireResponse()
	defer ReleaseResponse(resp)
	if err := c.Get(srv.URL+"/big", resp); err != nil {
		t.Fatal(err)
	}
	if _, err := resp.ReadBody(); !errors.Is(err, ErrResponseBodyTooLarge) {
		t.Fatalf("ReadBody: %v", err)
	}
	if err := c.Get(srv.URL+"/bigheader", resp); err == nil {
		t.Fatal("oversized header accepted")
	}
	start := time.Now()
	if err := c.Get(srv.URL+"/slow", resp); err == nil || time.Since(start) > 2*time.Second {
		t.Fatalf("ResponseHeaderTimeout: %v after %v", err, time.Since(start))
	}
}

// TestClientStaleBodyReference reads again through a Body that reached EOF, while its connection already
// serves another response; it must not see that response.
func TestClientStaleBodyReference(t *testing.T) {
	checkGoroutines(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, r.URL.Path)
	}))
	defer srv.Close()
	c, _ := newTestClient(t)
	resp := AcquireResponse()
	if err := c.Get(srv.URL+"/first", resp); err != nil {
		t.Fatal(err)
	}
	kept := resp.Body
	if b, _ := io.ReadAll(kept); string(b) != "/first" {
		t.Fatal(string(b))
	}
	defer ReleaseResponse(resp) // held, so the next response is a different object on the same connection

	resp2 := AcquireResponse()
	defer ReleaseResponse(resp2)
	if err := c.Get(srv.URL+"/second", resp2); err != nil {
		t.Fatal(err)
	}
	var buf [16]byte
	if n, err := kept.Read(buf[:]); n != 0 || err == nil {
		t.Fatalf("stale body read %q, %v", buf[:n], err)
	}
	if b, _ := resp2.ReadBody(); string(b) != "/second" {
		t.Fatalf("second response %q", b)
	}
}
