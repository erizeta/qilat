package qilat

import (
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
