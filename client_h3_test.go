package qilat

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/quic-go/quic-go/http3"
)

// startTCPAndQUIC serves h over TLS on TCP and over HTTP/3 on UDP, on the same port number as
// ListenAndServeTLS does, so responses carry Alt-Svc.
func startTCPAndQUIC(t *testing.T, h HandlerFunc) string {
	t.Helper()
	cert, err := GenerateDevCert()
	if err != nil {
		t.Fatal(err)
	}
	var ln net.Listener
	var pc net.PacketConn
	for range 20 { // find a port free on both TCP and UDP
		if ln, err = net.Listen("tcp", "127.0.0.1:0"); err != nil {
			t.Fatal(err)
		}
		if pc, err = net.ListenPacket("udp", ln.Addr().String()); err == nil {
			break
		}
		_ = ln.Close()
	}
	if pc == nil {
		t.Fatal("no port free on both TCP and UDP")
	}
	srv := &Server{Handler: h, TLSConfig: &tls.Config{Certificates: []tls.Certificate{cert}}}
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); _ = srv.ServeTLS(ln, "", "") }()
	go func() { defer wg.Done(); _ = srv.ServeQUIC(pc) }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
		wg.Wait()
	})
	waitAltSvc(t, srv)
	return ln.Addr().String()
}

// waitAltSvc waits until srv advertises HTTP/3, which ServeQUIC does once its listener is open. Until then,
// TCP responses carry no Alt-Svc.
func waitAltSvc(t *testing.T, srv *Server) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for srv.altSvc.Load() == nil {
		if time.Now().After(deadline) {
			t.Fatal("ServeQUIC did not start advertising HTTP/3")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestClientH3ViaAltSvc(t *testing.T) {
	checkGoroutines(t)
	canceled := make(chan struct{}, 1)
	addr := startTCPAndQUIC(t, func(w ResponseWriter, r *Request) {
		w.Header().Set("X-Proto", r.Proto())
		if string(r.Path()) == "/stream" {
			_, _ = w.WriteString("first")
			_ = w.Flush()
			<-r.Context().Done()
			canceled <- struct{}{}
			return
		}
		_, _ = io.Copy(w, r.Body)
		w.(TrailerSetter).Trailer().Set("X-Done", "1")
	})
	c, tr := insecureTransport(t, func(tr *Transport) { tr.EnableHTTP3 = true })
	base := "https://" + addr

	resp, _ := do(t, c, "GET", base+"/", "")
	if resp.Proto() != "HTTP/2.0" || resp.Header.Get("Alt-Svc") == "" {
		t.Fatalf("first request: %s %v", resp.Proto(), fields(resp.Header))
	}
	big := strings.Repeat("h3 body ", 200_000)
	resp, body := do(t, c, "POST", base+"/echo", big)
	if resp.Proto() != "HTTP/3.0" || resp.Header.Get("X-Proto") != "HTTP/3.0" || body != big ||
		resp.Trailer().Get("X-Done") != "1" {
		t.Fatalf("second request: %s %d bytes %v %v", resp.Proto(), len(body), fields(resp.Header), fields(resp.Trailer()))
	}

	ctx, cancel := context.WithCancel(context.Background())
	req := AcquireRequest()
	defer ReleaseRequest(req)
	_ = req.SetRequestURI(base + "/stream")
	resp2 := AcquireResponse()
	defer ReleaseResponse(resp2)
	if err := c.Do(req.WithContext(ctx), resp2); err != nil || resp2.Proto() != "HTTP/3.0" {
		t.Fatalf("%v %s", err, resp2.Proto())
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
	tr.CloseIdleConnections()
}

// TestClientH3AgainstReference reaches quic-go's http3.Server through an Alt-Svc advertised by a TCP server.
func TestClientH3AgainstReference(t *testing.T) {
	checkGoroutines(t)
	cert, err := GenerateDevCert()
	if err != nil {
		t.Fatal(err)
	}
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		w.Header().Set("X-Proto", r.Proto)
		_, _ = fmt.Fprintf(w, "%s %s %s", r.Method, r.URL.RequestURI(), body)
	})
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ref := &http3.Server{Handler: handler, TLSConfig: http3.ConfigureTLSConfig(&tls.Config{Certificates: []tls.Certificate{cert}})}
	done := make(chan struct{})
	go func() { defer close(done); _ = ref.Serve(pc) }()
	defer func() { _ = ref.Close(); <-done }()

	port := pc.LocalAddr().(*net.UDPAddr).Port
	tcp := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Alt-Svc", fmt.Sprintf(`h3=":%d"; ma=60`, port))
		handler(w, r)
	}))
	tcp.TLS = &tls.Config{Certificates: []tls.Certificate{cert}}
	tcp.StartTLS()
	defer tcp.Close()

	c, tr := insecureTransport(t, func(tr *Transport) { tr.EnableHTTP3 = true })
	defer tr.CloseIdleConnections()
	if resp, _ := do(t, c, "GET", tcp.URL+"/learn", ""); resp.Proto() == "HTTP/3.0" {
		t.Fatal("first request already used HTTP/3")
	}
	resp, body := do(t, c, "PUT", tcp.URL+"/x?y=1", "payload")
	if resp.Proto() != "HTTP/3.0" || resp.Header.Get("X-Proto") != "HTTP/3.0" || body != "PUT /x?y=1 payload" {
		t.Fatalf("%s %q %v", resp.Proto(), body, fields(resp.Header))
	}
}

// TestClientH3FallsBackToTCP advertises an HTTP/3 endpoint that drops every packet, as a firewall blocking
// UDP would.
func TestClientH3FallsBackToTCP(t *testing.T) {
	checkGoroutines(t)
	blackhole, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = blackhole.Close() }()
	port := blackhole.LocalAddr().(*net.UDPAddr).Port
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Alt-Svc", fmt.Sprintf(`h3=":%d"; ma=60`, port))
		_, _ = io.WriteString(w, r.Proto)
	}))
	srv.EnableHTTP2 = true
	srv.StartTLS()
	defer srv.Close()
	c, _ := insecureTransport(t, func(tr *Transport) {
		tr.EnableHTTP3 = true
		tr.HTTP3DialTimeout = 200 * time.Millisecond
	})
	if _, body := do(t, c, "GET", srv.URL, ""); body != "HTTP/2.0" {
		t.Fatal(body)
	}
	start := time.Now()
	if _, body := do(t, c, "GET", srv.URL, ""); body != "HTTP/2.0" {
		t.Fatalf("fallback: %q", body)
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("fallback took %v", d)
	}
	start = time.Now()
	if _, body := do(t, c, "GET", srv.URL, ""); body != "HTTP/2.0" {
		t.Fatal(body)
	}
	if d := time.Since(start); d > 150*time.Millisecond {
		t.Fatalf("HTTP/3 was tried again after failing (%v)", d)
	}
}

func TestParseAltSvc(t *testing.T) {
	for _, tt := range []struct {
		v, addr string
		ma      time.Duration
		ok      bool
	}{
		{`h3=":443"`, "example.com:443", 24 * time.Hour, true},
		{`h3=":8443"; ma=60`, "example.com:8443", time.Minute, true},
		{`h2=":443", h3="alt.example.com:443"; ma=3600; persist=1`, "alt.example.com:443", time.Hour, true},
		{`h3-29=":443"`, "", 0, false},
		{`clear`, "", 0, false},
		{`h3=443`, "", 0, false},
		{`h3=":"`, "", 0, false},
	} {
		addr, ma, ok := parseAltSvc([]byte(tt.v), "example.com:443")
		if addr != tt.addr || ma != tt.ma || ok != tt.ok {
			t.Errorf("%q: %q %v %v", tt.v, addr, ma, ok)
		}
	}
}

func TestClientH3UsesDialContext(t *testing.T) {
	checkGoroutines(t)
	addr := startTCPAndQUIC(t, func(w ResponseWriter, r *Request) { _, _ = w.WriteString(r.Proto()) })
	var mu sync.Mutex
	var networks []string
	var d net.Dialer
	c, _ := insecureTransport(t, func(tr *Transport) {
		tr.EnableHTTP3 = true
		tr.DialContext = func(ctx context.Context, network, a string) (net.Conn, error) {
			mu.Lock()
			networks = append(networks, network)
			mu.Unlock()
			return d.DialContext(ctx, network, a)
		}
	})
	for _, want := range []string{"HTTP/2.0", "HTTP/3.0", "HTTP/3.0"} {
		if _, body := do(t, c, "GET", "https://"+addr+"/", ""); body != want {
			t.Fatalf("got %s, want %s", body, want)
		}
	}
	mu.Lock()
	if got := strings.Join(networks, ","); got != "tcp,udp" {
		t.Fatalf("DialContext called with %s", got)
	}
	mu.Unlock()

	// A dialer that refuses UDP keeps requests on TCP.
	refuse, _ := insecureTransport(t, func(tr *Transport) {
		tr.EnableHTTP3 = true
		tr.DialContext = func(ctx context.Context, network, a string) (net.Conn, error) {
			if network == "udp" {
				return nil, errors.New("udp not allowed")
			}
			return d.DialContext(ctx, network, a)
		}
	})
	for range 3 {
		if _, body := do(t, refuse, "GET", "https://"+addr+"/", ""); body != "HTTP/2.0" {
			t.Fatalf("refused UDP: %s", body)
		}
	}
}
