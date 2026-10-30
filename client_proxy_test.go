package qilat

import (
	"bufio"
	"encoding/base64"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

// connectProxy is a minimal HTTP proxy: it tunnels CONNECT and answers plain requests itself, recording
// what it saw.
type connectProxy struct {
	ln       net.Listener
	connects atomic.Int32
	mu       sync.Mutex
	targets  []string
	auth     []string
	wg       sync.WaitGroup
}

func startConnectProxy(t *testing.T) *connectProxy {
	t.Helper()
	p := &connectProxy{ln: listenLocal(t)}
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		for {
			nc, err := p.ln.Accept()
			if err != nil {
				return
			}
			p.wg.Add(1)
			go func() { defer p.wg.Done(); p.serve(nc) }()
		}
	}()
	t.Cleanup(func() { _ = p.ln.Close(); p.wg.Wait() })
	return p
}

func (p *connectProxy) serve(nc net.Conn) {
	defer func() { _ = nc.Close() }()
	br := bufio.NewReader(nc)
	for {
		req, err := http.ReadRequest(br)
		if err != nil {
			return
		}
		p.mu.Lock()
		p.targets = append(p.targets, req.Method+" "+req.RequestURI)
		p.auth = append(p.auth, req.Header.Get("Proxy-Authorization"))
		p.mu.Unlock()
		if req.Method != "CONNECT" {
			_, _ = io.WriteString(nc, "HTTP/1.1 200 OK\r\nContent-Length: 7\r\n\r\nproxied")
			continue
		}
		p.connects.Add(1)
		upstream, err := net.Dial("tcp", req.RequestURI)
		if err != nil {
			_, _ = io.WriteString(nc, "HTTP/1.1 502 Bad Gateway\r\nContent-Length: 0\r\n\r\n")
			return
		}
		_, _ = io.WriteString(nc, "HTTP/1.1 200 Connection established\r\n\r\n")
		done := make(chan struct{})
		go func() { _, _ = io.Copy(upstream, br); _ = upstream.Close(); close(done) }()
		_, _ = io.Copy(nc, upstream)
		_ = nc.Close()
		<-done
		return
	}
}

func TestClientProxy(t *testing.T) {
	checkGoroutines(t)
	target := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "tunneled "+r.Proto)
	}))
	target.EnableHTTP2 = true
	target.StartTLS()
	defer target.Close()
	proxy := startConnectProxy(t)
	proxyURL, _ := url.Parse("http://user:pa%3Ass@" + proxy.ln.Addr().String())
	c, tr := insecureTransport(t, func(tr *Transport) {
		tr.Proxy = func(*Request) (*url.URL, error) { return proxyURL, nil }
		tr.EnableHTTP3 = true // never used through a proxy
	})

	if _, body := do(t, c, "GET", target.URL+"/a", ""); body != "tunneled HTTP/2.0" {
		t.Fatalf("https through proxy: %q", body)
	}
	if _, body := do(t, c, "GET", target.URL+"/b", ""); body != "tunneled HTTP/2.0" {
		t.Fatalf("second request: %q", body)
	}
	if n := proxy.connects.Load(); n != 1 {
		t.Fatalf("%d CONNECTs; the tunnel should be reused", n)
	}
	if _, body := do(t, c, "GET", "http://example.invalid/plain?x=1", ""); body != "proxied" {
		t.Fatalf("http through proxy: %q", body)
	}
	tr.CloseIdleConnections()
	wantAuth := "Basic " + base64.StdEncoding.EncodeToString([]byte("user:pa:ss"))
	proxy.mu.Lock()
	defer proxy.mu.Unlock()
	targets := strings.Join(proxy.targets, ", ")
	if targets != "CONNECT "+strings.TrimPrefix(target.URL, "https://")+", GET http://example.invalid/plain?x=1" {
		t.Fatalf("proxy saw %s", targets)
	}
	for _, a := range proxy.auth {
		if a != wantAuth {
			t.Fatalf("Proxy-Authorization %q, want %q", a, wantAuth)
		}
	}
}

func TestClientProxyRefused(t *testing.T) {
	checkGoroutines(t)
	proxy := startConnectProxy(t)
	proxyURL, _ := url.Parse("http://" + proxy.ln.Addr().String())
	c, _ := insecureTransport(t, func(tr *Transport) {
		tr.Proxy = func(*Request) (*url.URL, error) { return proxyURL, nil }
	})
	resp := AcquireResponse()
	defer ReleaseResponse(resp)
	// Nothing listens on port 1, so the proxy answers 502.
	if err := c.Get("https://127.0.0.1:1/", resp); err == nil || !strings.Contains(err.Error(), "502") {
		t.Fatalf("err %v", err)
	}
}
