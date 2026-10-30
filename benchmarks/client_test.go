package benchmarks

import (
	"bytes"
	"io"
	"net/http"
	"runtime"
	"testing"

	"github.com/erizeta/qilat"
	"github.com/valyala/fasthttp"
)

// clientImpl sends one scenario request and returns the response body.
type clientImpl struct {
	name string
	// newDo returns a function used by one goroutine; state that cannot be shared lives in its closure.
	newDo func(base string, sc scenario) func() ([]byte, error)
	close func()
}

// newClients builds every client under test with a connection pool large enough for the benchmark's
// goroutines, so pool limits do not decide the result.
func newClients() []clientImpl {
	conns := *parallelism * runtime.GOMAXPROCS(0)
	qt := &qilat.Transport{MaxIdleConnsPerHost: conns}
	qc := &qilat.Client{Transport: qt}
	nt := &http.Transport{MaxIdleConns: conns, MaxIdleConnsPerHost: conns}
	nc := &http.Client{Transport: nt}
	fc := &fasthttp.Client{MaxConnsPerHost: conns}
	return []clientImpl{
		{name: "nethttp", close: nt.CloseIdleConnections, newDo: func(base string, sc scenario) func() ([]byte, error) {
			url := base + sc.path
			var buf bytes.Buffer
			return func() ([]byte, error) {
				var body io.Reader
				if sc.body != nil {
					body = bytes.NewReader(sc.body)
				}
				req, err := http.NewRequest(sc.method, url, body)
				if err != nil {
					return nil, err
				}
				for _, h := range sc.headers {
					req.Header.Set(h[0], h[1])
				}
				resp, err := nc.Do(req)
				if err != nil {
					return nil, err
				}
				defer func() { _ = resp.Body.Close() }()
				buf.Reset()
				_, err = buf.ReadFrom(resp.Body)
				return buf.Bytes(), err
			}
		}},
		{name: "fasthttp", close: fc.CloseIdleConnections, newDo: func(base string, sc scenario) func() ([]byte, error) {
			req, resp := fasthttp.AcquireRequest(), fasthttp.AcquireResponse()
			req.SetRequestURI(base + sc.path)
			req.Header.SetMethod(sc.method)
			for _, h := range sc.headers {
				req.Header.Set(h[0], h[1])
			}
			return func() ([]byte, error) {
				if sc.body != nil {
					req.SetBodyRaw(sc.body)
				}
				if err := fc.Do(req, resp); err != nil {
					return nil, err
				}
				return resp.Body(), nil
			}
		}},
		{name: "qilat", close: qt.CloseIdleConnections, newDo: func(base string, sc scenario) func() ([]byte, error) {
			req, resp := qilat.AcquireRequest(), qilat.AcquireResponse()
			req.SetMethod(sc.method)
			if err := req.SetRequestURI(base + sc.path); err != nil {
				panic(err)
			}
			for _, h := range sc.headers {
				req.Header.Set(h[0], h[1])
			}
			body := bytes.NewReader(nil)
			return func() ([]byte, error) {
				if sc.body != nil {
					body.Reset(sc.body)
					req.SetBody(body, int64(len(sc.body)))
				}
				if err := qc.Do(req, resp); err != nil {
					return nil, err
				}
				b, err := resp.ReadBody()
				_ = resp.Close()
				return b, err
			}
		}},
	}
}

// TestClients checks that every client gets the expected body for every scenario.
func TestClients(t *testing.T) {
	ln := listen(t)
	defer startFastHTTP(t, ln)()
	base := "http://" + ln.Addr().String()
	for _, c := range newClients() {
		for _, sc := range scenarios {
			do := c.newDo(base, sc)
			for range 2 {
				got, err := do()
				if err != nil {
					t.Fatalf("%s %s: %v", c.name, sc.name, err)
				}
				if !bytes.Equal(got, sc.wantBody) {
					t.Fatalf("%s %s: body %q", c.name, sc.name, got)
				}
			}
		}
		c.close()
	}
}

// BenchmarkClientH1 measures HTTP/1.1 clients against the same fasthttp server over loopback, with
// keep-alive connections. Lower is better.
func BenchmarkClientH1(b *testing.B) {
	ln := listen(b)
	defer startFastHTTP(b, ln)()
	base := "http://" + ln.Addr().String()
	for _, sc := range scenarios {
		for _, c := range newClients() {
			b.Run(sc.name+"/"+c.name, func(b *testing.B) {
				b.SetParallelism(*parallelism)
				b.ReportAllocs()
				b.RunParallel(func(pb *testing.PB) {
					do := c.newDo(base, sc)
					for pb.Next() {
						if _, err := do(); err != nil {
							b.Error(err)
							return
						}
					}
				})
			})
			c.close()
		}
	}
}
