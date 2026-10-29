package benchmarks

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"testing"

	"github.com/valyala/fasthttp/fasthttputil"
)

// With only GOMAXPROCS connections the loopback round trip, not the server, limits throughput.
var parallelism = flag.Int("parallelism", 8, "client connections per GOMAXPROCS for each benchmark")

// h1Mode is how the client drives an HTTP/1.1 server.
type h1Mode struct {
	name     string
	inmem    bool // in-memory listener: no kernel, so the numbers reflect server CPU cost
	newConn  bool // dial a new connection per request
	pipeline int  // requests per write; 1 means no pipelining
}

var h1Modes = []h1Mode{
	{name: "keepalive", pipeline: 1},
	{name: "newconn", newConn: true, pipeline: 1},
	{name: "pipeline16", pipeline: 16},
	{name: "inmem", inmem: true, pipeline: 1},
}

// skip reports whether the combination is not meaningful; a pipelined 64 KB upload mostly measures socket buffers.
func (m h1Mode) skip(sc scenario) bool {
	return m.pipeline > 1 && sc.body != nil
}

// h1Listener returns a listener for the mode and a dial function connected to it.
func h1Listener(tb testing.TB, m h1Mode) (net.Listener, func() (net.Conn, error)) {
	if m.inmem {
		ln := fasthttputil.NewInmemoryListener()
		return ln, ln.Dial
	}
	ln := listen(tb)
	addr := ln.Addr().String()
	return ln, func() (net.Conn, error) { return net.Dial("tcp", addr) }
}

// TestScenarios checks that every server answers every scenario correctly, so benchmarks compare equal work.
func TestScenarios(t *testing.T) {
	for _, srv := range h1Servers {
		for _, sc := range scenarios {
			for _, m := range h1Modes {
				if m.skip(sc) || m.newConn {
					continue
				}
				t.Run("h1/"+srv.name+"/"+sc.name+"/"+m.name, func(t *testing.T) {
					ln, dial := h1Listener(t, m)
					defer srv.start(t, ln)()
					c, err := dialH1(dial)
					if err != nil {
						t.Fatal(err)
					}
					defer func() { _ = c.Close() }()
					req := sc.rawRequest()
					if m.pipeline > 1 {
						if err := c.doPipelined(bytes.Repeat(req, m.pipeline), m.pipeline, len(sc.wantBody)); err != nil {
							t.Fatal(err)
						}
					}
					// Twice on the same connection to cover keep-alive.
					for range 2 {
						got, err := c.do(req)
						if err != nil {
							t.Fatal(err)
						}
						if !bytes.Equal(got, sc.wantBody) {
							t.Fatalf("body = %q, want %q", got, sc.wantBody)
						}
					}
				})
			}
		}
	}
	for _, srv := range h2Servers {
		for _, sc := range scenarios {
			t.Run("h2c/"+srv.name+"/"+sc.name, func(t *testing.T) {
				ln := listen(t)
				defer srv.start(t, ln)()
				client := newH2CClient()
				defer client.CloseIdleConnections()
				protoMajor, got, err := doH2(client, ln.Addr().String(), sc)
				if err != nil {
					t.Fatal(err)
				}
				if protoMajor != 2 {
					t.Fatalf("proto major = %d, want 2", protoMajor)
				}
				if !bytes.Equal(got, sc.wantBody) {
					t.Fatalf("body = %q, want %q", got, sc.wantBody)
				}
			})
		}
	}
}

func BenchmarkH1(b *testing.B) {
	for _, srv := range h1Servers {
		for _, sc := range scenarios {
			for _, m := range h1Modes {
				if m.skip(sc) {
					continue
				}
				b.Run(fmt.Sprintf("%s/%s/%s", srv.name, sc.name, m.name), func(b *testing.B) {
					ln, dial := h1Listener(b, m)
					defer srv.start(b, ln)()
					benchH1(b, dial, sc, m)
				})
			}
		}
	}
}

func benchH1(b *testing.B, dial func() (net.Conn, error), sc scenario, m h1Mode) {
	req := sc.rawRequest()
	batch := bytes.Repeat(req, m.pipeline)
	b.SetBytes(int64(len(req)))
	b.SetParallelism(*parallelism)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		c, err := dialH1(dial)
		if err != nil {
			b.Error(err)
			return
		}
		defer func() { _ = c.Close() }()
		pending := 0
		for pb.Next() {
			if m.pipeline > 1 {
				if pending++; pending < m.pipeline {
					continue
				}
				pending = 0
				if err := c.doPipelined(batch, m.pipeline, len(sc.wantBody)); err != nil {
					b.Error(err)
					return
				}
				continue
			}
			got, err := c.do(req)
			if err != nil {
				b.Error(err)
				return
			}
			if len(got) != len(sc.wantBody) {
				b.Errorf("body length = %d, want %d", len(got), len(sc.wantBody))
				return
			}
			if m.newConn {
				_ = c.Close()
			}
		}
		if pending > 0 {
			if err := c.doPipelined(batch[:pending*len(req)], pending, len(sc.wantBody)); err != nil {
				b.Error(err)
			}
		}
	})
}

// BenchmarkH2C uses cleartext HTTP/2 with prior knowledge so TLS cost does not hide protocol cost.
// The client is net/http, so client allocations are included in the numbers.
func BenchmarkH2C(b *testing.B) {
	for _, srv := range h2Servers {
		for _, sc := range scenarios {
			b.Run(fmt.Sprintf("%s/%s", srv.name, sc.name), func(b *testing.B) {
				ln := listen(b)
				defer srv.start(b, ln)()
				client := newH2CClient()
				defer client.CloseIdleConnections()
				addr := ln.Addr().String()
				b.SetBytes(int64(len(sc.rawRequest())))
				b.SetParallelism(*parallelism)
				b.ReportAllocs()
				b.ResetTimer()
				b.RunParallel(func(pb *testing.PB) {
					for pb.Next() {
						_, got, err := doH2(client, addr, sc)
						if err != nil {
							b.Error(err)
							return
						}
						if len(got) != len(sc.wantBody) {
							b.Errorf("body length = %d, want %d", len(got), len(sc.wantBody))
							return
						}
					}
				})
			})
		}
	}
}

func newH2CClient() *http.Client {
	var p http.Protocols
	p.SetUnencryptedHTTP2(true)
	return &http.Client{Transport: &http.Transport{Protocols: &p}}
}

// doH2 sends the scenario and returns the response protocol major version and body.
func doH2(client *http.Client, addr string, sc scenario) (int, []byte, error) {
	var body io.Reader
	if sc.body != nil {
		body = bytes.NewReader(sc.body)
	}
	req, err := http.NewRequest(sc.method, "http://"+addr+sc.path, body)
	if err != nil {
		return 0, nil, err
	}
	for _, h := range sc.headers {
		req.Header.Set(h[0], h[1])
	}
	resp, err := client.Do(req)
	if err != nil {
		return 0, nil, err
	}
	got, err := io.ReadAll(resp.Body)
	if cerr := resp.Body.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return 0, nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return 0, nil, fmt.Errorf("status = %d", resp.StatusCode)
	}
	return resp.ProtoMajor, got, nil
}
