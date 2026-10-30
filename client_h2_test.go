package qilat

import (
	"bytes"
	"context"
	"crypto/tls"
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

	"golang.org/x/net/http2"
	"golang.org/x/net/http2/hpack"
)

func insecureTransport(t *testing.T, configure ...func(*Transport)) (*Client, *Transport) {
	t.Helper()
	return newTestClient(t, append([]func(*Transport){func(tr *Transport) {
		tr.TLSClientConfig = &tls.Config{InsecureSkipVerify: true} //nolint:gosec // self-signed test certificate
	}}, configure...)...)
}

func TestClientH2AgainstNetHTTP(t *testing.T) {
	checkGoroutines(t)
	var conns atomic.Int32
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Proto", r.Proto)
		w.Header().Set("X-Authority", r.Host)
		switch r.URL.Path {
		case "/echo":
			w.Header().Set("Trailer", "X-Len")
			n, _ := io.Copy(w, r.Body)
			w.Header().Set("X-Len", strings.Repeat("x", int(n%7)))
		case "/empty":
			w.WriteHeader(204)
		case "/slow":
			time.Sleep(20 * time.Millisecond)
			_, _ = io.WriteString(w, "slow")
		}
	}))
	srv.EnableHTTP2 = true
	srv.Config.ConnState = func(_ net.Conn, s http.ConnState) {
		if s == http.StateNew {
			conns.Add(1)
		}
	}
	srv.StartTLS()
	defer srv.Close()
	c, _ := insecureTransport(t)

	big := strings.Repeat("abcdefgh", 300_000) // larger than every default flow-control window
	resp, body := do(t, c, "POST", srv.URL+"/echo?q=1", big)
	if resp.Proto() != "HTTP/2.0" || resp.Header.Get("X-Proto") != "HTTP/2.0" || body != big ||
		resp.Trailer().Get("X-Len") != strings.Repeat("x", len(big)%7) ||
		resp.Header.Get("X-Authority") != strings.TrimPrefix(srv.URL, "https://") {
		t.Fatalf("echo: %s %d bytes %v trailer %v", resp.Proto(), len(body), fields(resp.Header), fields(resp.Trailer()))
	}
	if resp, body = do(t, c, "GET", srv.URL+"/empty", ""); resp.StatusCode() != 204 || body != "" {
		t.Fatalf("204: %d %q", resp.StatusCode(), body)
	}
	if resp, body = do(t, c, "HEAD", srv.URL+"/slow", ""); resp.StatusCode() != 200 || body != "" {
		t.Fatalf("HEAD: %d %q", resp.StatusCode(), body)
	}

	var wg sync.WaitGroup
	for range 50 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp := AcquireResponse()
			defer ReleaseResponse(resp)
			if err := c.Get(srv.URL+"/slow", resp); err != nil {
				t.Error(err)
				return
			}
			if b, err := resp.ReadBody(); err != nil || string(b) != "slow" {
				t.Errorf("%q %v", b, err)
			}
		}()
	}
	wg.Wait()
	if n := conns.Load(); n != 1 {
		t.Fatalf("%d connections; HTTP/2 requests should share one", n)
	}
}

func TestClientH2AgainstQilat(t *testing.T) {
	cert, err := GenerateDevCert()
	if err != nil {
		t.Fatal(err)
	}
	canceled := make(chan struct{}, 1)
	addr := startTLSServer(t, func(w ResponseWriter, r *Request) {
		switch string(r.Path()) {
		case "/stream":
			_, _ = w.WriteString("first")
			_ = w.Flush()
			<-r.Context().Done()
			canceled <- struct{}{}
		default:
			w.Header().Set("X-Proto", r.Proto())
			_, _ = io.Copy(w, r.Body)
			w.(TrailerSetter).Trailer().Set("X-Done", "1")
		}
	}, cert)
	c, _ := insecureTransport(t)
	big := strings.Repeat("0123456789", 200_000)
	resp, body := do(t, c, "PUT", "https://"+addr+"/echo", big)
	if resp.Proto() != "HTTP/2.0" || resp.Header.Get("X-Proto") != "HTTP/2.0" || body != big || resp.Trailer().Get("X-Done") != "1" {
		t.Fatalf("%s %d bytes %v %v", resp.Proto(), len(body), fields(resp.Header), fields(resp.Trailer()))
	}

	// Canceling mid-body resets the stream: the handler's context ends and the connection stays usable.
	ctx, cancel := context.WithCancel(context.Background())
	req := AcquireRequest()
	defer ReleaseRequest(req)
	_ = req.SetRequestURI("https://" + addr + "/stream")
	resp2 := AcquireResponse()
	defer ReleaseResponse(resp2)
	if err := c.Do(req.WithContext(ctx), resp2); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 5)
	if _, err := io.ReadFull(resp2.Body, buf); err != nil {
		t.Fatal(err)
	}
	cancel()
	if _, err := resp2.Body.Read(buf); !errors.Is(err, context.Canceled) {
		t.Fatalf("read after cancel: %v", err)
	}
	select {
	case <-canceled:
	case <-time.After(5 * time.Second):
		t.Fatal("server handler was not canceled")
	}
	if _, body := do(t, c, "POST", "https://"+addr+"/echo", "again"); body != "again" {
		t.Fatalf("after cancel: %q", body)
	}
}

func TestClientDisableHTTP2(t *testing.T) {
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, r.Proto)
	}))
	srv.EnableHTTP2 = true
	srv.StartTLS()
	defer srv.Close()
	c, _ := insecureTransport(t, func(tr *Transport) { tr.DisableHTTP2 = true })
	if resp, body := do(t, c, "GET", srv.URL, ""); body != "HTTP/1.1" || resp.Proto() != "HTTP/1.1" {
		t.Fatalf("%s %q", resp.Proto(), body)
	}
}

func TestClientH2C(t *testing.T) {
	checkGoroutines(t)
	ts := startH2C(t, func(w ResponseWriter, r *Request) {
		w.Header().Set("X-Proto", r.Proto())
		_, _ = io.Copy(w, r.Body)
	})
	c, _ := newTestClient(t, func(tr *Transport) { tr.H2C = true })
	resp, body := do(t, c, "POST", ts.url("/echo"), "over h2c")
	if resp.Proto() != "HTTP/2.0" || resp.Header.Get("X-Proto") != "HTTP/2.0" || body != "over h2c" {
		t.Fatalf("qilat h2c: %s %q %v", resp.Proto(), body, fields(resp.Header))
	}

	var conns atomic.Int32
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(10 * time.Millisecond)
		_, _ = io.WriteString(w, r.Proto)
	}))
	srv.Config.Protocols = new(http.Protocols)
	srv.Config.Protocols.SetHTTP1(true)
	srv.Config.Protocols.SetUnencryptedHTTP2(true)
	srv.Config.ConnState = func(_ net.Conn, s http.ConnState) {
		if s == http.StateNew {
			conns.Add(1)
		}
	}
	srv.Start()
	defer srv.Close()
	var wg sync.WaitGroup
	for range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp := AcquireResponse()
			defer ReleaseResponse(resp)
			if err := c.Get(srv.URL, resp); err != nil {
				t.Error(err)
				return
			}
			if b, _ := resp.ReadBody(); string(b) != "HTTP/2.0" {
				t.Errorf("net/http h2c: %q", b)
			}
		}()
	}
	wg.Wait()
	if n := conns.Load(); n != 1 {
		t.Fatalf("%d connections for h2c requests", n)
	}

	plain, _ := newTestClient(t)
	if resp, _ := do(t, plain, "GET", srv.URL, ""); resp.Proto() != "HTTP/1.1" {
		t.Fatalf("without H2C: %s", resp.Proto())
	}
}

// countingTLSServer starts an HTTPS server that counts its connections.
func countingTLSServer(t *testing.T, h2 bool, h http.HandlerFunc) (*httptest.Server, *atomic.Int32) {
	t.Helper()
	var conns atomic.Int32
	srv := httptest.NewUnstartedServer(h)
	srv.EnableHTTP2 = h2
	srv.Config.ConnState = func(_ net.Conn, s http.ConnState) {
		if s == http.StateNew {
			conns.Add(1)
		}
	}
	srv.StartTLS()
	t.Cleanup(srv.Close)
	return srv, &conns
}

func burst(t *testing.T, c *Client, url string, n int) []error {
	t.Helper()
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp := AcquireResponse()
			defer ReleaseResponse(resp)
			if errs[i] = c.Get(url, resp); errs[i] == nil {
				_, errs[i] = resp.ReadBody()
			}
		}()
	}
	wg.Wait()
	return errs
}

// TestClientFirstBurstSharesHTTP2Dial sends concurrent first requests to a new HTTPS host: they wait for
// one connection instead of each opening its own.
func TestClientFirstBurstSharesHTTP2Dial(t *testing.T) {
	checkGoroutines(t)
	srv, conns := countingTLSServer(t, true, func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, r.Proto)
	})
	c, _ := insecureTransport(t)
	for _, err := range burst(t, c, srv.URL, 20) {
		if err != nil {
			t.Fatal(err)
		}
	}
	if n := conns.Load(); n != 1 {
		t.Fatalf("%d connections for a first burst to an HTTP/2 host", n)
	}
}

func TestClientFirstBurstToHTTP1Host(t *testing.T) {
	checkGoroutines(t)
	srv, conns := countingTLSServer(t, false, func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(20 * time.Millisecond)
		_, _ = io.WriteString(w, r.Proto)
	})
	c, tr := insecureTransport(t)
	for _, err := range burst(t, c, srv.URL, 10) {
		if err != nil {
			t.Fatal(err)
		}
	}
	if n := conns.Load(); n < 2 {
		t.Fatalf("%d connections: waiters did not dial their own once the host spoke HTTP/1.1", n)
	}
	hc := tr.hostConns("https", []byte(strings.TrimPrefix(srv.URL, "https://")), nil)
	hc.mu.Lock()
	proto := hc.proto
	hc.mu.Unlock()
	if proto != protoH1 {
		t.Fatalf("host protocol %d, want HTTP/1.1", proto)
	}
}

// TestClientSharedDialFailure fails the shared first dial: only its own request fails, and the waiters
// dial for themselves at once instead of one after another.
func TestClientSharedDialFailure(t *testing.T) {
	checkGoroutines(t)
	srv, _ := countingTLSServer(t, true, func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "ok")
	})
	var dials atomic.Int32
	var d net.Dialer
	c, _ := insecureTransport(t, func(tr *Transport) {
		tr.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
			if dials.Add(1) == 1 {
				time.Sleep(50 * time.Millisecond) // let the burst queue up behind this dial
				return nil, errors.New("first dial fails")
			}
			time.Sleep(50 * time.Millisecond)
			return d.DialContext(ctx, network, addr)
		}
	})
	start := time.Now()
	failed := 0
	for _, err := range burst(t, c, srv.URL, 10) {
		if err != nil {
			failed++
		}
	}
	if failed != 1 {
		t.Fatalf("%d requests failed, want only the one whose dial failed", failed)
	}
	// Serialized retries would take about 10 × 50 ms.
	if d := time.Since(start); d > 400*time.Millisecond {
		t.Fatalf("waiters took %v; they should dial in parallel after the shared dial fails", d)
	}
}

// TestClientH2ResponseHeaderLimit answers with a block of one-byte references to a 4 KB table entry, which
// decodes to far more than MaxResponseHeaderBytes. The stream must fail without the connection losing HPACK
// sync, so the next response, which references the same entry, still decodes.
func TestClientH2ResponseHeaderLimit(t *testing.T) {
	checkGoroutines(t)
	big := hpack.HeaderField{Name: "x-big", Value: strings.Repeat("a", 4000)}
	var hb bytes.Buffer
	enc := hpack.NewEncoder(&hb)
	block := func(refs int) []byte {
		hb.Reset()
		_ = enc.WriteField(hpack.HeaderField{Name: ":status", Value: "200"})
		for range refs {
			_ = enc.WriteField(big)
		}
		return bytes.Clone(hb.Bytes())
	}
	blocks := [][]byte{block(1000), block(1)}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	go func() {
		sc, err := ln.Accept()
		if err != nil {
			return
		}
		defer func() { _ = sc.Close() }()
		if _, err := io.ReadFull(sc, make([]byte, len(http2.ClientPreface))); err != nil {
			return
		}
		fr := http2.NewFramer(sc, sc)
		_ = fr.WriteSettings()
		for {
			f, err := fr.ReadFrame()
			if err != nil {
				return
			}
			switch f := f.(type) {
			case *http2.SettingsFrame:
				if !f.IsAck() {
					_ = fr.WriteSettingsAck()
				}
			case *http2.HeadersFrame:
				if len(blocks) == 0 {
					return
				}
				_ = fr.WriteHeaders(http2.HeadersFrameParam{StreamID: f.StreamID, BlockFragment: blocks[0],
					EndHeaders: true, EndStream: true})
				blocks = blocks[1:]
			}
		}
	}()
	c, _ := newTestClient(t, func(tr *Transport) { tr.H2C = true })
	url := "http://" + ln.Addr().String() + "/"
	resp := AcquireResponse()
	defer ReleaseResponse(resp)
	if err := c.Get(url, resp); err == nil || !strings.Contains(err.Error(), "MaxHeaderListSize") {
		t.Fatalf("oversized header block: %v", err)
	}
	if err := c.Get(url, resp); err != nil || resp.Header.Get("X-Big") != big.Value {
		t.Fatalf("next response on the connection: %v, x-big of %d bytes", err, len(resp.Header.Get("X-Big")))
	}
}

// TestClientRedirectDowngradeDropsCredentials follows https://a.test/ to http://a.test/: same host, but the
// credentials would cross the network in cleartext.
func TestClientRedirectDowngradeDropsCredentials(t *testing.T) {
	checkGoroutines(t)
	plain := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "auth="+r.Header.Get("Authorization")+" cookie="+r.Header.Get("Cookie"))
	}))
	defer plain.Close()
	secure := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "http://a.test/", http.StatusFound)
	}))
	defer secure.Close()
	c, _ := insecureTransport(t, func(tr *Transport) {
		tr.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
			target := plain.Listener.Addr().String()
			if addr == "a.test:443" {
				target = secure.Listener.Addr().String()
			}
			return (&net.Dialer{}).DialContext(ctx, network, target)
		}
	})
	_, body := do(t, c, "GET", "https://a.test/", "", [2]string{"Authorization", "secret"}, [2]string{"Cookie", "s=1"})
	if body != "auth= cookie=" {
		t.Fatalf("after https to http redirect: %q", body)
	}
}
