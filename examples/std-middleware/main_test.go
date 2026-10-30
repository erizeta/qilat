package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"io"
	"log"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/erizeta/qilat"
	"github.com/quic-go/quic-go/http3"
)

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

// TestMiddlewareOnAllProtocols runs chi and rs/cors through qilat's adapter on HTTP/1.1, HTTP/2, and HTTP/3.
func TestMiddlewareOnAllProtocols(t *testing.T) {
	logs := &syncBuffer{}
	cert, err := qilat.GenerateDevCert()
	if err != nil {
		t.Fatal(err)
	}
	srv := &qilat.Server{
		Handler:   newHandler(log.New(logs, "", 0)),
		TLSConfig: &tls.Config{Certificates: []tls.Certificate{cert}},
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
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

	for _, c := range []struct {
		name, base, proto string
		rt                http.RoundTripper
	}{
		{"h1", "https://" + ln.Addr().String(), "HTTP/1.1", h1},
		{"h2", "https://" + ln.Addr().String(), "HTTP/2.0", h2},
		{"h3", "https://" + pc.LocalAddr().String(), "HTTP/3.0", h3},
	} {
		client := &http.Client{Transport: c.rt, Timeout: 10 * time.Second}
		do := func(method, path string, header map[string]string) (int, http.Header, string) {
			t.Helper()
			req, _ := http.NewRequest(method, c.base+path, nil)
			for k, v := range header {
				req.Header.Set(k, v)
			}
			resp, err := client.Do(req)
			if err != nil {
				t.Fatalf("%s %s: %v", c.name, path, err)
			}
			defer func() { _ = resp.Body.Close() }()
			body, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Fatal(err)
			}
			return resp.StatusCode, resp.Header, string(body)
		}

		status, header, body := do("GET", "/hello/qilat", map[string]string{"Origin": "https://example.com"})
		if status != 200 || header.Get("Access-Control-Allow-Origin") != "https://example.com" ||
			header.Get("X-Content-Type-Options") != "nosniff" ||
			!strings.HasPrefix(body, "hello qilat over "+c.proto+", request ") || strings.HasSuffix(body, "request \n") {
			t.Errorf("%s GET: %d %v %q", c.name, status, header, body)
		}

		status, header, _ = do("GET", "/hello/x", map[string]string{"Origin": "https://evil.example"})
		if status != 200 || header.Get("Access-Control-Allow-Origin") != "" {
			t.Errorf("%s disallowed origin: %d %v", c.name, status, header)
		}

		status, header, _ = do("OPTIONS", "/hello/x", map[string]string{
			"Origin": "https://example.com", "Access-Control-Request-Method": "POST",
		})
		if status != 204 || header.Get("Access-Control-Allow-Methods") != "POST" {
			t.Errorf("%s preflight: %d %v", c.name, status, header)
		}

		// Heartbeat answers in the middleware, without reaching the qilat handler.
		if status, _, body = do("GET", "/ping", nil); status != 200 || body != "." {
			t.Errorf("%s ping: %d %q", c.name, status, body)
		}

		if status, _, _ = do("GET", "/panic", nil); status != 500 {
			t.Errorf("%s panic: %d", c.name, status)
		}
	}
	// chi's logger wraps the ResponseWriter and records the status the qilat handler wrote.
	if logs := logs.String(); !strings.Contains(logs, `"GET https://`) || !strings.Contains(logs, " - 204 ") ||
		!strings.Contains(logs, " - 500 ") {
		t.Errorf("logs:\n%s", logs)
	}
}
