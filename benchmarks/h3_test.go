package benchmarks

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
	"testing"
	"time"

	"github.com/erizeta/qilat"
	"github.com/quic-go/quic-go/http3"
)

// h3Server starts an HTTP/3 server on pc and returns a stop function.
type h3Server struct {
	name  string
	start func(tb testing.TB, pc net.PacketConn, cert tls.Certificate) func()
}

var h3Servers = []h3Server{
	{name: "quicgo", start: startQUICGo},
	{name: "qilat", start: startQilatH3},
}

func startQUICGo(tb testing.TB, pc net.PacketConn, cert tls.Certificate) func() {
	srv := &http3.Server{
		Handler:   netHTTPHandler(),
		TLSConfig: http3.ConfigureTLSConfig(&tls.Config{Certificates: []tls.Certificate{cert}}),
	}
	done := make(chan error, 1)
	go func() { done <- srv.Serve(pc) }()
	return func() {
		_ = srv.Close()
		if err := <-done; err != nil && !errors.Is(err, http.ErrServerClosed) && !errors.Is(err, net.ErrClosed) {
			tb.Errorf("quic-go serve: %v", err)
		}
		_ = pc.Close()
	}
}

func startQilatH3(tb testing.TB, pc net.PacketConn, cert tls.Certificate) func() {
	srv := &qilat.Server{
		Handler:   qilat.HandlerFunc(qilatHandler),
		TLSConfig: &tls.Config{Certificates: []tls.Certificate{cert}},
		Logger:    slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	done := make(chan error, 1)
	go func() { done <- srv.ServeQUIC(pc) }()
	return func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := srv.Shutdown(ctx); err != nil {
			tb.Errorf("qilat shutdown: %v", err)
		}
		if err := <-done; !errors.Is(err, qilat.ErrServerClosed) {
			tb.Errorf("qilat serve: %v", err)
		}
	}
}

// BenchmarkH3 compares HTTP/3 servers with the quic-go/http3 client, whose cost is included in the numbers.
func BenchmarkH3(b *testing.B) {
	cert, err := qilat.GenerateDevCert()
	if err != nil {
		b.Fatal(err)
	}
	for _, srv := range h3Servers {
		for _, sc := range scenarios {
			b.Run(fmt.Sprintf("%s/%s", srv.name, sc.name), func(b *testing.B) {
				pc, err := net.ListenPacket("udp", "127.0.0.1:0")
				if err != nil {
					b.Fatal(err)
				}
				defer srv.start(b, pc, cert)()
				tr := &http3.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}} //nolint:gosec // dev certificate
				defer func() { _ = tr.Close() }()
				client := &http.Client{Transport: tr}
				base := "https://" + pc.LocalAddr().String()
				b.SetBytes(int64(len(sc.rawRequest())))
				b.SetParallelism(*parallelism)
				b.ReportAllocs()
				b.ResetTimer()
				b.RunParallel(func(pb *testing.PB) {
					for pb.Next() {
						var body io.Reader
						if sc.body != nil {
							body = bytes.NewReader(sc.body)
						}
						req, _ := http.NewRequest(sc.method, base+sc.path, body)
						for _, h := range sc.headers {
							req.Header.Set(h[0], h[1])
						}
						resp, err := client.Do(req)
						if err != nil {
							b.Error(err)
							return
						}
						got, err := io.ReadAll(resp.Body)
						_ = resp.Body.Close()
						if err != nil || len(got) != len(sc.wantBody) {
							b.Errorf("status %d body %q (%d bytes), want %d: %v", resp.StatusCode, got, len(got), len(sc.wantBody), err)
							return
						}
					}
				})
			})
		}
	}
}
