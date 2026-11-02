package benchmarks

import (
	"crypto/tls"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/erizeta/qilat"
)

// BenchmarkClientStatusOnly sends sequential HTTPS requests over HTTP/1.1 that look only at the status and close
// the response with its 64 KiB body unread, the pattern Go 1.27's response drain speeds up. conns/op is the
// share of requests that needed a new connection, and so a TLS handshake.
func BenchmarkClientStatusOnly(b *testing.B) {
	body := strings.Repeat("x", 64<<10)
	var conns atomic.Int64
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, body)
	}))
	srv.EnableHTTP2 = false
	srv.Config.ConnState = func(_ net.Conn, s http.ConnState) {
		if s == http.StateNew {
			conns.Add(1)
		}
	}
	srv.StartTLS()
	defer srv.Close()
	insecure := &tls.Config{InsecureSkipVerify: true} //nolint:gosec // test certificate

	run := func(b *testing.B, get func() error) {
		b.ReportAllocs()
		start := conns.Load()
		b.ResetTimer()
		for range b.N {
			if err := get(); err != nil {
				b.Fatal(err)
			}
		}
		b.StopTimer()
		b.ReportMetric(float64(conns.Load()-start)/float64(b.N), "conns/op")
	}

	b.Run("nethttp", func(b *testing.B) {
		tr := &http.Transport{TLSClientConfig: insecure.Clone()}
		defer tr.CloseIdleConnections()
		client := &http.Client{Transport: tr}
		run(b, func() error {
			resp, err := client.Get(srv.URL)
			if err != nil {
				return err
			}
			return resp.Body.Close()
		})
	})
	b.Run("qilat", func(b *testing.B) {
		tr := &qilat.Transport{TLSClientConfig: insecure.Clone()}
		defer tr.CloseIdleConnections()
		client := &qilat.Client{Transport: tr}
		run(b, func() error {
			resp := qilat.AcquireResponse()
			defer qilat.ReleaseResponse(resp)
			return client.Get(srv.URL, resp)
		})
	})
}
