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
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/net/http2"
	"golang.org/x/net/http2/hpack"
)

// h2cClient returns a net/http client speaking cleartext HTTP/2 with prior knowledge.
func h2cClient(t *testing.T) *http.Client {
	t.Helper()
	tr := &http.Transport{Protocols: new(http.Protocols)}
	tr.Protocols.SetUnencryptedHTTP2(true)
	t.Cleanup(tr.CloseIdleConnections)
	return &http.Client{Transport: tr, Timeout: 10 * time.Second}
}

func startH2C(t *testing.T, h HandlerFunc, configure ...func(*Server)) *testServer {
	t.Helper()
	return startServer(t, h, append([]func(*Server){func(s *Server) { s.H2C = true }}, configure...)...)
}

func TestHTTP2Basic(t *testing.T) {
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
	ts := startH2C(t, mux.ServeHTTP)
	client := h2cClient(t)

	resp, err := client.Get(ts.url("/hello/qilat?x=1"))
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.ProtoMajor != 2 || resp.StatusCode != 200 || string(body) != "hello qilat" ||
		resp.Header.Get("X-Proto") != "HTTP/2.0" || resp.ContentLength != 11 || resp.Header.Get("Date") == "" {
		t.Fatalf("GET: proto %d status %d body %q header %v cl %d", resp.ProtoMajor, resp.StatusCode, body,
			resp.Header, resp.ContentLength)
	}

	// Larger than the default 64 KiB windows in both directions, so flow control must work.
	payload := bytes.Repeat([]byte("0123456789abcdef"), 300<<10/16)
	resp, err = client.Post(ts.url("/echo"), "application/octet-stream", bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	body, err = io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if err != nil || !bytes.Equal(body, payload) {
		t.Fatalf("echo: %d bytes, err %v", len(body), err)
	}

	resp, err = client.Get(ts.url("/trailer"))
	if err != nil {
		t.Fatal(err)
	}
	body, _ = io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if string(body) != "body" || resp.Trailer.Get("Grpc-Status") != "0" {
		t.Fatalf("trailer: body %q trailer %v", body, resp.Trailer)
	}

	req, _ := http.NewRequest("HEAD", ts.url("/hello/x"), nil)
	resp, err = client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != 200 || resp.ContentLength != 7 {
		t.Fatalf("HEAD: status %d cl %d", resp.StatusCode, resp.ContentLength)
	}

	resp, err = client.Get(ts.url("/missing"))
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != 404 {
		t.Fatalf("missing: status %d", resp.StatusCode)
	}
}

// startTLSServer serves h with ServeTLS and returns the address.
func startTLSServer(t *testing.T, h HandlerFunc, cert tls.Certificate, configure ...func(*Server)) string {
	t.Helper()
	checkGoroutines(t)
	srv := &Server{
		Handler:   h,
		TLSConfig: &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12},
		ErrorLog:  slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	for _, f := range configure {
		f(srv)
	}
	ln := listenLocal(t)
	done := make(chan error, 1)
	go func() { done <- srv.ServeTLS(ln, "", "") }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
		if err := <-done; !errors.Is(err, ErrServerClosed) {
			t.Errorf("ServeTLS = %v", err)
		}
	})
	return ln.Addr().String()
}

func TestHTTP2OverTLS(t *testing.T) {
	cert := selfSignedCert(t)
	h := func(w ResponseWriter, r *Request) { _, _ = w.WriteString(r.Proto()) }
	for _, tt := range []struct {
		name    string
		disable bool
		want    string
	}{{"alpn h2", false, "HTTP/2.0"}, {"DisableHTTP2", true, "HTTP/1.1"}} {
		t.Run(tt.name, func(t *testing.T) {
			addr := startTLSServer(t, h, cert, func(s *Server) { s.DisableHTTP2 = tt.disable })
			tr := &http.Transport{
				TLSClientConfig:   &tls.Config{InsecureSkipVerify: true}, //nolint:gosec // self-signed test certificate
				ForceAttemptHTTP2: true,
			}
			defer tr.CloseIdleConnections()
			resp, err := (&http.Client{Transport: tr, Timeout: 5 * time.Second}).Get("https://" + addr + "/")
			if err != nil {
				t.Fatal(err)
			}
			body, _ := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			if string(body) != tt.want || resp.Proto != tt.want {
				t.Fatalf("proto %s body %q", resp.Proto, body)
			}
		})
	}
}

func TestHTTP2ConcurrentStreams(t *testing.T) {
	ts := startH2C(t, func(w ResponseWriter, r *Request) {
		_, _ = w.WriteString(strings.Repeat(r.URI().Query().Get("c"), 20000))
	})
	client := h2cClient(t)
	var wg sync.WaitGroup
	errs := make(chan error, 200)
	for i := range 200 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c := string(rune('a' + i%26))
			resp, err := client.Get(ts.url("/?c=" + c))
			if err != nil {
				errs <- err
				return
			}
			body, err := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			if err != nil || string(body) != strings.Repeat(c, 20000) || resp.ProtoMajor != 2 {
				errs <- fmt.Errorf("stream %d: %d bytes, proto %d, err %v", i, len(body), resp.ProtoMajor, err)
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

func TestHTTP2ClientCancelCancelsContext(t *testing.T) {
	started := make(chan struct{})
	done := make(chan error, 1)
	ts := startH2C(t, func(w ResponseWriter, r *Request) {
		close(started)
		select {
		case <-r.Context().Done():
			done <- r.Context().Err()
		case <-time.After(5 * time.Second):
			done <- errors.New("context not canceled")
		}
	})
	ctx, cancel := context.WithCancel(context.Background())
	req, _ := http.NewRequestWithContext(ctx, "GET", ts.url("/"), nil)
	go func() {
		<-started
		cancel()
	}()
	if resp, err := h2cClient(t).Do(req); err == nil {
		_ = resp.Body.Close()
	}
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("handler context: %v", err)
	}
}

func TestHTTP2ShutdownWithActiveStream(t *testing.T) {
	release := make(chan struct{})
	started := make(chan struct{})
	ts := startH2C(t, func(w ResponseWriter, r *Request) {
		close(started)
		<-release
		_, _ = w.WriteString("finished")
	})
	client := h2cClient(t)
	result := make(chan string, 1)
	go func() {
		resp, err := client.Get(ts.url("/"))
		if err != nil {
			result <- err.Error()
			return
		}
		body, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		result <- string(body)
	}()
	<-started
	shutdown := make(chan error, 1)
	go func() { shutdown <- ts.Shutdown(context.Background()) }()
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

func TestHTTP2ShutdownDeadlineCancelsStreams(t *testing.T) {
	canceled := make(chan struct{})
	started := make(chan struct{})
	ts := startH2C(t, func(w ResponseWriter, r *Request) {
		close(started)
		<-r.Context().Done()
		close(canceled)
	})
	go func() {
		resp, err := h2cClient(t).Get(ts.url("/"))
		if err == nil {
			_ = resp.Body.Close()
		}
	}()
	<-started
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := ts.Shutdown(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Shutdown = %v", err)
	}
	<-canceled
	_ = ts.Close()
}

func TestHTTP1Trailers(t *testing.T) {
	ts := startServer(t, func(w ResponseWriter, r *Request) {
		w.(TrailerSetter).Trailer().Set("X-Checksum", "abc")
		_, _ = w.WriteString("body")
	})
	c := dialRaw(t, ts.addr)
	c.send("GET / HTTP/1.1\r\nHost: x\r\n\r\n")
	resp, err := http.ReadResponse(c.br, nil)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if string(body) != "body" || resp.Trailer.Get("X-Checksum") != "abc" || len(resp.TransferEncoding) == 0 {
		t.Fatalf("body %q trailer %v te %v", body, resp.Trailer, resp.TransferEncoding)
	}
}

func TestHTTP2RequestTrailersAndCookies(t *testing.T) {
	ts := startH2C(t, func(w ResponseWriter, r *Request) {
		body, _ := io.ReadAll(r.Body)
		_, _ = w.WriteString(string(body) + "|" + r.Header.Get("cookie"))
	})
	req, _ := http.NewRequest("POST", ts.url("/"), io.NopCloser(strings.NewReader("data")))
	req.Trailer = http.Header{"X-T": {"v"}}
	req.Header.Add("Cookie", "a=1")
	req.AddCookie(&http.Cookie{Name: "b", Value: "2"})
	resp, err := h2cClient(t).Do(req)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if string(body) != "data|a=1; b=2" {
		t.Fatalf("body %q", body)
	}
}

// TestH2WriteTimeoutStalledClient opens large flow-control windows, then stops reading. The handler's writes
// must fail within WriteTimeout instead of blocking the connection until the client reads again.
func TestH2WriteTimeoutStalledClient(t *testing.T) {
	failed := make(chan time.Duration, 1)
	ts := startServer(t, func(w ResponseWriter, _ *Request) {
		start := time.Now()
		chunk := make([]byte, 64<<10)
		for range 2048 {
			if _, err := w.Write(chunk); err != nil {
				failed <- time.Since(start)
				return
			}
		}
	}, func(s *Server) { s.H2C, s.WriteTimeout = true, 200*time.Millisecond })
	nc, err := net.Dial("tcp", ts.addr)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = nc.Close() }()
	_, _ = io.WriteString(nc, http2.ClientPreface)
	fr := http2.NewFramer(nc, nc)
	_ = fr.WriteSettings(http2.Setting{ID: http2.SettingInitialWindowSize, Val: 1<<31 - 1})
	_ = fr.WriteWindowUpdate(0, 1<<31-1-65535)
	var hb bytes.Buffer
	enc := hpack.NewEncoder(&hb)
	for _, kv := range [][2]string{{":method", "GET"}, {":scheme", "http"}, {":path", "/"}, {":authority", "x"}} {
		_ = enc.WriteField(hpack.HeaderField{Name: kv[0], Value: kv[1]})
	}
	_ = fr.WriteHeaders(http2.HeadersFrameParam{StreamID: 1, BlockFragment: hb.Bytes(), EndHeaders: true, EndStream: true})
	select {
	case d := <-failed:
		if d > 2*time.Second {
			t.Fatalf("write failed after %v", d)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("handler still blocked writing to a client that stopped reading")
	}
}
