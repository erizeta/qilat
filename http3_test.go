package qilat

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/quic-go/quic-go/http3"
)

// startQUIC serves h over HTTP/3 on a loopback UDP port and returns the server and its address.
func startQUIC(t *testing.T, h HandlerFunc, configure ...func(*Server)) (*Server, string) {
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
	for _, f := range configure {
		f(srv)
	}
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- srv.ServeQUIC(pc) }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
		if err := <-done; !errors.Is(err, ErrServerClosed) {
			t.Errorf("ServeQUIC = %v", err)
		}
	})
	return srv, pc.LocalAddr().String()
}

func h3Client(t *testing.T) *http.Client {
	t.Helper()
	tr := &http3.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}} //nolint:gosec // self-signed test certificate
	t.Cleanup(func() { _ = tr.Close() })
	return &http.Client{Transport: tr, Timeout: 10 * time.Second}
}

func readAll(t *testing.T, resp *http.Response, err error) string {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func TestHTTP3Basic(t *testing.T) {
	mux := NewServeMux()
	mux.HandleFunc("GET /hello/{name}", func(w ResponseWriter, r *Request) {
		w.Header().Set("Content-Type", "text/plain")
		w.Header().Set("X-Proto", r.Proto())
		_, _ = w.WriteString("hello " + r.PathValue("name"))
	})
	mux.HandleFunc("POST /echo", func(w ResponseWriter, r *Request) {
		_, _ = io.Copy(w, r.Body)
	})
	mux.HandleFunc("GET /trailer", func(w ResponseWriter, r *Request) {
		_, _ = w.WriteString("body")
		w.(TrailerSetter).Trailer().Set("Grpc-Status", "0")
	})
	mux.HandleFunc("GET /stream", func(w ResponseWriter, r *Request) {
		for range 3 {
			_, _ = w.Write(bytes.Repeat([]byte("s"), 8<<10))
			_ = w.Flush()
		}
	})
	_, addr := startQUIC(t, mux.ServeHTTP)
	client := h3Client(t)
	base := "https://" + addr

	resp, err := client.Get(base + "/hello/qilat") //nolint:bodyclose // readAll closes the body
	if body := readAll(t, resp, err); body != "hello qilat" || resp.Proto != "HTTP/3.0" ||
		resp.Header.Get("X-Proto") != "HTTP/3.0" || resp.ContentLength != 11 || resp.Header.Get("Date") == "" {
		t.Fatalf("GET: %s %q %v cl=%d", resp.Proto, body, resp.Header, resp.ContentLength)
	}
	if resp.Header.Get("Alt-Svc") != "" {
		t.Fatal("HTTP/3 response advertises Alt-Svc")
	}

	payload := bytes.Repeat([]byte("0123456789abcdef"), 1<<20/16)
	resp, err = client.Post(base+"/echo", "application/octet-stream", bytes.NewReader(payload)) //nolint:bodyclose // readAll closes the body
	if body := readAll(t, resp, err); body != string(payload) {
		t.Fatalf("echo: %d bytes", len(body))
	}

	resp, err = client.Get(base + "/trailer") //nolint:bodyclose // readAll closes the body
	if body := readAll(t, resp, err); body != "body" || resp.Trailer.Get("Grpc-Status") != "0" {
		t.Fatalf("trailer: %q %v", body, resp.Trailer)
	}

	resp, err = client.Get(base + "/stream") //nolint:bodyclose // readAll closes the body
	if body := readAll(t, resp, err); len(body) != 3*8<<10 {
		t.Fatalf("stream: %d bytes", len(body))
	}

	req, _ := http.NewRequest("HEAD", base+"/hello/x", nil)
	resp, err = client.Do(req) //nolint:bodyclose // readAll closes the body
	if body := readAll(t, resp, err); body != "" || resp.StatusCode != 200 || resp.ContentLength != 7 {
		t.Fatalf("HEAD: %d cl=%d %q", resp.StatusCode, resp.ContentLength, body)
	}

	resp, err = client.Get(base + "/missing") //nolint:bodyclose // readAll closes the body
	if readAll(t, resp, err); resp.StatusCode != 404 {
		t.Fatalf("missing: %d", resp.StatusCode)
	}
}

// TestSameResponseOnAllProtocols serves one handler over HTTP/1.1, HTTP/2, and HTTP/3 and checks that the
// status, header fields (apart from Date and Alt-Svc), and body are identical.
func TestSameResponseOnAllProtocols(t *testing.T) {
	mux := NewServeMux()
	mux.HandleFunc("GET /items/{id}", func(w ResponseWriter, r *Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Add("Vary", "Accept")
		w.Header().Add("Vary", "Origin")
		w.Header().Set("Cache-Control", "no-store")
		_, _ = w.WriteString(`{"id":"` + r.PathValue("id") + `","q":"` + r.URI().Query().Get("q") + `"}`)
	})
	mux.HandleFunc("POST /items", func(w ResponseWriter, r *Request) {
		body, _ := io.ReadAll(r.Body)
		w.Header().Set("Location", "/items/1")
		w.WriteHeader(201)
		_, _ = w.Write(bytes.ToUpper(body))
	})
	mux.HandleFunc("GET /big", func(w ResponseWriter, r *Request) {
		_, _ = w.Write(bytes.Repeat([]byte("x"), 100<<10))
	})
	cert, err := GenerateDevCert()
	if err != nil {
		t.Fatal(err)
	}
	srv := &Server{
		Handler:   mux,
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
	defer func() {
		_ = srv.Shutdown(context.Background())
		wg.Wait()
	}()
	waitAltSvc(t, srv)

	insecure := &tls.Config{InsecureSkipVerify: true} //nolint:gosec // self-signed test certificate
	h1 := &http.Transport{TLSClientConfig: insecure.Clone()}
	h2 := &http.Transport{TLSClientConfig: insecure.Clone(), ForceAttemptHTTP2: true}
	h3 := &http3.Transport{TLSClientConfig: insecure.Clone()}
	defer h1.CloseIdleConnections()
	defer h2.CloseIdleConnections()
	defer func() { _ = h3.Close() }()
	clients := []struct {
		name, base, proto string
		rt                http.RoundTripper
	}{
		{"h1", "https://" + ln.Addr().String(), "HTTP/1.1", h1},
		{"h2", "https://" + ln.Addr().String(), "HTTP/2.0", h2},
		{"h3", "https://" + pc.LocalAddr().String(), "HTTP/3.0", h3},
	}
	type result struct {
		status int
		header string
		body   string
	}
	for _, rq := range []struct{ method, path, body string }{
		{"GET", "/items/42?q=a%20b", ""},
		{"POST", "/items", "new item"},
		{"GET", "/big", ""},
		{"GET", "/nope", ""},
		{"DELETE", "/items/1", ""},
	} {
		var want result
		for i, c := range clients {
			var body io.Reader
			if rq.body != "" {
				body = strings.NewReader(rq.body)
			}
			req, _ := http.NewRequest(rq.method, c.base+rq.path, body)
			resp, err := (&http.Client{Transport: c.rt, Timeout: 10 * time.Second}).Do(req) //nolint:bodyclose // readAll closes the body
			b := readAll(t, resp, err)
			if resp.Proto != c.proto {
				t.Fatalf("%s %s: proto %s", c.name, rq.path, resp.Proto)
			}
			if c.name != "h3" && resp.Header.Get("Alt-Svc") != fmt.Sprintf(`h3=":%d"; ma=86400`, pc.LocalAddr().(*net.UDPAddr).Port) {
				t.Errorf("%s: Alt-Svc %q", c.name, resp.Header.Get("Alt-Svc"))
			}
			var fields []string
			for k, vs := range resp.Header {
				if k != "Date" && k != "Alt-Svc" {
					fields = append(fields, k+"="+strings.Join(vs, ","))
				}
			}
			slices.Sort(fields)
			got := result{resp.StatusCode, strings.Join(fields, "; "), b}
			if i == 0 {
				want = got
			} else if got != want {
				t.Errorf("%s %s %s:\n got %d %s %q\nwant %d %s %q", c.name, rq.method, rq.path,
					got.status, got.header, trim(got.body), want.status, want.header, trim(want.body))
			}
		}
	}
}

func trim(s string) string {
	if len(s) > 40 {
		return s[:40] + "..."
	}
	return s
}

func TestHTTP3ClientCancelCancelsContext(t *testing.T) {
	started := make(chan struct{})
	done := make(chan error, 1)
	_, addr := startQUIC(t, func(w ResponseWriter, r *Request) {
		close(started)
		select {
		case <-r.Context().Done():
			done <- r.Context().Err()
		case <-time.After(5 * time.Second):
			done <- errors.New("context not canceled")
		}
	})
	ctx, cancel := context.WithCancel(context.Background())
	req, _ := http.NewRequestWithContext(ctx, "GET", "https://"+addr+"/", nil)
	go func() {
		<-started
		cancel()
	}()
	if resp, err := h3Client(t).Do(req); err == nil {
		_ = resp.Body.Close()
	}
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("handler context: %v", err)
	}
}

func TestHTTP3ShutdownWithActiveStream(t *testing.T) {
	release := make(chan struct{})
	started := make(chan struct{})
	srv, addr := startQUIC(t, func(w ResponseWriter, r *Request) {
		close(started)
		<-release
		_, _ = w.WriteString("finished")
	})
	result := make(chan string, 1)
	go func() {
		resp, err := h3Client(t).Get("https://" + addr + "/")
		if err != nil {
			result <- err.Error()
			return
		}
		b, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		result <- string(b)
	}()
	<-started
	shutdown := make(chan error, 1)
	go func() { shutdown <- srv.Shutdown(context.Background()) }()
	select {
	case err := <-shutdown:
		t.Fatalf("Shutdown returned %v with a stream still running", err)
	case <-time.After(100 * time.Millisecond):
	}
	close(release)
	if got := <-result; got != "finished" {
		t.Fatalf("in-flight request got %q", got)
	}
	if err := <-shutdown; err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
}

func TestListenAndServeTLSFailsWhenUDPPortIsTaken(t *testing.T) {
	checkGoroutines(t)
	busy, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = busy.Close() }()
	addr := busy.LocalAddr().String()
	cert, err := GenerateDevCert()
	if err != nil {
		t.Fatal(err)
	}
	srv := &Server{Handler: HandlerFunc(func(ResponseWriter, *Request) {}),
		TLSConfig: &tls.Config{Certificates: []tls.Certificate{cert}}}
	errc := make(chan error, 1)
	go func() { errc <- srv.ListenAndServeTLS(addr, "", "") }()
	select {
	case err := <-errc:
		if err == nil || errors.Is(err, ErrServerClosed) {
			t.Fatalf("ListenAndServeTLS = %v, want the UDP bind error", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("ListenAndServeTLS did not fail")
	}
	// The TCP listener must not be left running.
	if c, err := net.DialTimeout("tcp", addr, time.Second); err == nil {
		_ = c.Close()
		t.Fatal("TCP listener still accepts connections")
	}
}

func TestListenAndServeTLSServesAllProtocols(t *testing.T) {
	checkGoroutines(t)
	// Find a port free for both TCP and UDP.
	var addr string
	for range 10 {
		ln := listenLocal(t)
		addr = ln.Addr().String()
		_ = ln.Close()
		if pc, err := net.ListenPacket("udp", addr); err == nil {
			_ = pc.Close()
			break
		}
	}
	cert, err := GenerateDevCert()
	if err != nil {
		t.Fatal(err)
	}
	srv := &Server{Handler: HandlerFunc(func(w ResponseWriter, r *Request) { _, _ = w.WriteString(r.Proto()) }),
		TLSConfig: &tls.Config{Certificates: []tls.Certificate{cert}},
		ErrorLog:  slog.New(slog.NewTextHandler(io.Discard, nil))}
	errc := make(chan error, 1)
	go func() { errc <- srv.ListenAndServeTLS(addr, "", "") }()
	insecure := &tls.Config{InsecureSkipVerify: true} //nolint:gosec // self-signed test certificate
	h2 := &http.Transport{TLSClientConfig: insecure, ForceAttemptHTTP2: true}
	h3 := &http3.Transport{TLSClientConfig: insecure.Clone()}
	defer h2.CloseIdleConnections()
	defer func() { _ = h3.Close() }()
	var got []string
	for _, rt := range []http.RoundTripper{h2, h3} {
		var resp *http.Response
		for range 50 { // the listeners start asynchronously
			//nolint:bodyclose // readAll closes the body
			if resp, err = (&http.Client{Transport: rt, Timeout: 2 * time.Second}).Get("https://" + addr + "/"); err == nil {
				break
			}
			time.Sleep(20 * time.Millisecond)
		}
		got = append(got, readAll(t, resp, err))
	}
	if got[0] != "HTTP/2.0" || got[1] != "HTTP/3.0" {
		t.Fatalf("protocols %v", got)
	}
	if err := srv.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := <-errc; !errors.Is(err, ErrServerClosed) {
		t.Fatalf("ListenAndServeTLS = %v", err)
	}
}

func TestGenerateDevCert(t *testing.T) {
	cert, err := GenerateDevCert("example.test", "10.0.0.1")
	if err != nil {
		t.Fatal(err)
	}
	if cert.Leaf.VerifyHostname("example.test") != nil || cert.Leaf.VerifyHostname("10.0.0.1") != nil ||
		cert.Leaf.VerifyHostname("localhost") == nil {
		t.Fatalf("names %v %v", cert.Leaf.DNSNames, cert.Leaf.IPAddresses)
	}
}
