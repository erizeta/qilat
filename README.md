# qilat

[![Test](https://github.com/erizeta/qilat/actions/workflows/test.yml/badge.svg)](https://github.com/erizeta/qilat/actions/workflows/test.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/erizeta/qilat.svg)](https://pkg.go.dev/github.com/erizeta/qilat)
[![Go Report Card](https://goreportcard.com/badge/github.com/erizeta/qilat)](https://goreportcard.com/report/github.com/erizeta/qilat)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

High-performance HTTP library for Go with **net/http-style APIs** and native **HTTP/1.1, HTTP/2, and HTTP/3** support.

> [!WARNING]
> **Work in progress.** qilat is in early development and is not usable yet. There are no releases,
> and the API will change without notice until v1.0.0. Follow the [roadmap](#roadmap) for progress.

## Why qilat?

[fasthttp](https://github.com/valyala/fasthttp) is fast thanks to zero-allocation parsing and aggressive
pooling, but it only speaks HTTP/1.1 and uses its own API that does not fit the net/http ecosystem.
[net/http](https://pkg.go.dev/net/http) supports HTTP/2 and has a familiar API, but allocates more per request
and has no built-in HTTP/3.

qilat aims for both:

| | net/http | fasthttp | qilat (goal) |
|---|---|---|---|
| HTTP/1.1 | ✅ | ✅ | ✅ |
| HTTP/2 | ✅ | ❌ | ✅ native |
| HTTP/3 (QUIC) | ❌ | ❌ | ✅ native |
| net/http-style `Handler` / `ResponseWriter` | ✅ | ❌ | ✅ |
| `ServeMux` with Go 1.22 patterns (`GET /users/{id}`) | ✅ | ❌ | ✅ allocation-free |
| Per-request `context.Context` cancellation | ✅ | ❌ (shutdown only) | ✅ allocation-free |
| Zero-allocation hot path | ❌ | ✅ | ✅ |
| Streaming request/response bodies | ✅ | partial | ✅ |
| Adapters to and from `net/http` | — | partial | ✅ |
| `FileServer` with Range, conditional requests, and sendfile | ✅ | ✅ | ✅ |
| Response compression (gzip, pluggable zstd/br) | ❌ | ✅ | ✅ allocation-free |
| HTTP client | ✅ H1, H2 | ✅ H1 | ✅ H1, H2, H3, allocation-free |

Every claim above will be backed by reproducible benchmarks in [`benchmarks/`](benchmarks/) before release.

## Requirements

- Go **1.26** or newer. CI tests the two latest Go releases.

## Installation

```sh
go get github.com/erizeta/qilat
```

## Quick start

qilat serves **HTTP/1.1**, **HTTP/2** (over TLS, and cleartext h2c), and **HTTP/3** with the same handlers.

```go
package main

import (
	"log"

	"github.com/erizeta/qilat"
)

func main() {
	srv := &qilat.Server{
		Handler: qilat.HandlerFunc(func(w qilat.ResponseWriter, r *qilat.Request) {
			name := r.URI().Query().Get("name")
			if name == "" {
				name = "World"
			}
			w.Header().Set("Content-Type", "text/plain; charset=utf-8")
			_, _ = w.WriteString("Hello, " + name + "!\n")
		}),
	}
	log.Fatal(srv.ListenAndServe(":8080"))
}
```

```sh
curl 'http://localhost:8080/?name=qilat'   # Hello, qilat!
```

[`examples/hello`](examples/hello/main.go) is the same server with graceful shutdown on Ctrl+C: `go run ./examples/hello`.

### What the server does

- Keep-alive, pipelining, chunked request and response bodies, `Expect: 100-continue`, HEAD, HTTP/1.0 clients.
- Strict RFC 9112 parsing that rejects request smuggling patterns (Content-Length with Transfer-Encoding,
  duplicate lengths, obsolete line folding, bare LF, and more) with the right status code.
- `Shutdown` drains active requests and closes idle connections; `Close` stops everything at once.
- `r.Context()` is canceled when the client disconnects, the handler returns, or a `Shutdown` deadline passes.
- `w.WriteHeader(103)` sends Early Hints; `w.(qilat.Hijacker).Hijack()` takes over the connection.
- Handler panics are recovered and logged; panic with `qilat.ErrAbortHandler` to abort quietly.
- Zero allocations per request on keep-alive connections. See [benchmarks](benchmarks/RESULTS.md).

### Safe defaults

Unlike net/http, a zero value means a safe limit, not "unlimited". Set a field to a negative value to disable it.

| Field | Zero value means |
|---|---|
| `ReadHeaderTimeout` | 10 s |
| `IdleTimeout` | 120 s |
| `MaxHeaderBytes` | 64 KiB (larger headers get 431) |
| `MaxRequestBodySize` | 4 MiB (larger bodies get 413, or `qilat.ErrBodyTooLarge` while reading) |
| `ReadTimeout`, `WriteTimeout` | no limit, so slow uploads and streaming responses work |
| `MaxConcurrentConns` | no limit |

### Building blocks

`Header`, `URI`, and `Args` are usable on their own and allocation-free in steady state:

| Type | What it does |
|---|---|
| `Header` | Ordered header fields, lowercase names, case-insensitive lookup, header-injection safe |
| `URI` | Strict request-target parsing (origin, absolute, authority, asterisk forms), lazy path decoding |
| `Args` | Query and form decoding compatible with `net/url.ParseQuery` |

[`examples/core`](examples/core/main.go) shows them in use: `go run ./examples/core`.

### Routing

`qilat.ServeMux` has the same pattern syntax, precedence, and redirect behavior as `net/http.ServeMux` (Go 1.22+),
and matching a request does not allocate:

```go
package main

import (
	"log"

	"github.com/erizeta/qilat"
)

func main() {
	mux := qilat.NewServeMux()
	mux.HandleFunc("GET /users/{id}", func(w qilat.ResponseWriter, r *qilat.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.WriteString(`{"id":"` + r.PathValue("id") + `"}` + "\n")
	})
	mux.HandleFunc("GET /files/{path...}", func(w qilat.ResponseWriter, r *qilat.Request) {
		_, _ = w.WriteString("file: " + r.PathValue("path") + "\n")
	})
	log.Fatal((&qilat.Server{Handler: mux}).ListenAndServe(":8080"))
}
```

```sh
curl localhost:8080/users/42           # {"id":"42"}
curl localhost:8080/files/docs/a.txt   # file: docs/a.txt
curl -i -X POST localhost:8080/users/42  # 405 Method Not Allowed, Allow: GET, HEAD
```

- Patterns: `[METHOD ][HOST]/path`, with `{name}` for one segment, `{name...}` for the rest of the path,
  `{$}` for an exact match, and a trailing `/` for a subtree. `GET` also matches `HEAD`.
- The most specific pattern wins, and registering two patterns that conflict panics with a description of both.
- Unclean paths (`/a/../b`, `//`) and subtree roots without the trailing slash are redirected with 307,
  unmatched paths get 404, and paths that match only another method get 405 with an `Allow` header.
- `r.PathValue(name)` returns a decoded copy that is safe to keep. `r.PeekPathValue(name)` returns the same
  bytes without copying, valid until the handler returns. `r.Pattern()` returns the matched pattern.

[`examples/routing`](examples/routing/main.go) is a small key-value API: `go run ./examples/routing`.

One deliberate difference from net/http: escapes such as `%2F` and `%2E` are never treated as `/` or `.`,
even in paths that also contain characters net/http re-escapes (where it would decode them before routing).

### HTTP/2

HTTP/2 needs no extra code. `ListenAndServeTLS` offers `h2` through ALPN and falls back to HTTP/1.1, and the same
handlers serve both. Set `H2C: true` to also accept cleartext HTTP/2 (prior knowledge) on a plain listener:

```go
package main

import (
	"log"

	"github.com/erizeta/qilat"
)

func main() {
	h := qilat.HandlerFunc(func(w qilat.ResponseWriter, r *qilat.Request) {
		_, _ = w.WriteString("hello over " + r.Proto() + "\n")
		// Trailers are sent after the body, as gRPC does with grpc-status.
		w.(qilat.TrailerSetter).Trailer().Set("X-Checksum", "abc123")
	})
	srv := &qilat.Server{Handler: h, H2C: true}
	log.Fatal(srv.ListenAndServe(":8080"))
}
```

```sh
curl --http2-prior-knowledge localhost:8080   # hello over HTTP/2.0
curl localhost:8080                           # hello over HTTP/1.1 (same port)
```

[`examples/http2`](examples/http2/main.go) serves HTTPS with a generated development certificate and h2c side by
side: `go run ./examples/http2`, then `curl -k --http2 https://localhost:8443`.

- Passes the [h2spec](https://github.com/summerwind/h2spec) conformance suite (147/147 in strict mode over TLS),
  run in CI.
- Request contexts are canceled when the client resets the stream; `Shutdown` sends GOAWAY and lets running
  streams finish.
- Flow control follows how fast handlers read request bodies, so a slow handler slows its client down instead of
  buffering without bound.
- Responses are scheduled by RFC 9218 priority (`priority` header and `PRIORITY_UPDATE`).
- Protections against Rapid Reset (CVE-2023-44487), CONTINUATION floods, and control-frame floods are always on.
- `Server.HTTP2` tunes limits (`MaxConcurrentStreams`, receive buffers, frame size); zero values are safe defaults.
  `DisableHTTP2` serves HTTP/1.1 only.

Trailers work on HTTP/1.1 too, in a chunked body. They are dropped only when the handler sets `Content-Length`
itself, or for HTTP/1.0 clients.

### HTTP/3

`ListenAndServeTLS` also serves HTTP/3 over UDP on the same port, and adds an `Alt-Svc` header to HTTP/1.1 and
HTTP/2 responses so browsers and curl switch to HTTP/3 on their next request:

```go
package main

import (
	"crypto/tls"
	"log"

	"github.com/erizeta/qilat"
)

func main() {
	cert, err := qilat.GenerateDevCert() // self-signed, for local development only
	if err != nil {
		log.Fatal(err)
	}
	srv := &qilat.Server{
		Handler: qilat.HandlerFunc(func(w qilat.ResponseWriter, r *qilat.Request) {
			_, _ = w.WriteString("hello over " + r.Proto() + "\n")
		}),
		TLSConfig: &tls.Config{Certificates: []tls.Certificate{cert}},
	}
	// TCP: HTTP/1.1 and HTTP/2. UDP: HTTP/3. Both on port 8443.
	log.Fatal(srv.ListenAndServeTLS(":8443", "", ""))
}
```

```sh
curl -k --http3-only https://localhost:8443   # hello over HTTP/3.0 (needs a curl built with HTTP/3)
curl -k https://localhost:8443                # hello over HTTP/2.0, with alt-svc: h3=":8443"; ma=86400
```

The same program is in [`examples/h3-dev-cert`](examples/h3-dev-cert/main.go): `go run ./examples/h3-dev-cert`.
In production, pass your certificate files instead: `srv.ListenAndServeTLS(":443", "cert.pem", "key.pem")`.

- If either the TCP or the UDP listener fails, `ListenAndServeTLS` closes the other and returns the error.
- `ServeQUIC(packetConn)` serves HTTP/3 on a UDP socket of your own; `DisableHTTP3` keeps `ListenAndServeTLS`
  on TCP only.
- `Server.HTTP3` tunes stream and flow-control limits and the Alt-Svc max age, or turns Alt-Svc off.
- 0-RTT is off by default. With `HTTP3.Allow0RTT`, returning clients save a round trip, and requests that
  arrive in 0-RTT with a method that is not idempotent are answered `425 Too Early`, since 0-RTT data can be
  replayed.
- Request contexts are canceled when the client resets the stream; `Shutdown` sends GOAWAY, lets running
  requests finish, then closes the connection.
- QPACK uses only the static table, so it needs no extra streams and decodes requests without allocating.
- QUIC needs UDP: open the UDP port in firewalls and load balancers. On Linux, quic-go may ask you to raise
  `net.core.rmem_max` for large receive buffers.

### net/http handlers and middleware

`qilat.FromStd` runs any `http.Handler` on qilat, over all three protocols, and `qilat.ToStd` mounts a qilat
handler on a net/http server. `qilat.FromStdMiddleware` adapts `func(http.Handler) http.Handler` middleware, so
packages such as [chi/middleware](https://github.com/go-chi/chi) and [rs/cors](https://github.com/rs/cors)
work unchanged:

```go
package main

import (
	"log"

	"github.com/erizeta/qilat"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/rs/cors"
)

func main() {
	mux := qilat.NewServeMux()
	mux.HandleFunc("GET /hello/{name}", func(w qilat.ResponseWriter, r *qilat.Request) {
		_, _ = w.WriteString("hello " + r.PathValue("name") + ", request " + middleware.GetReqID(r.Context()) + "\n")
	})
	// Compose the net/http middleware first, then adapt the chain once.
	std := chi.Chain(
		middleware.RequestID,
		middleware.Heartbeat("/ping"),
		middleware.Recoverer,
		cors.New(cors.Options{AllowedOrigins: []string{"https://example.com"}}).Handler,
	)
	h := qilat.FromStdMiddleware(std.Handler)(mux)
	log.Fatal((&qilat.Server{Handler: h}).ListenAndServe(":8080"))
}
```

```sh
go get github.com/go-chi/chi/v5 github.com/rs/cors
curl -i -H 'Origin: https://example.com' localhost:8080/hello/qilat   # Access-Control-Allow-Origin: https://example.com
```

[`examples/std-middleware`](examples/std-middleware/main.go) is a separate module (so qilat does not depend on chi
or cors) whose test runs this chain over HTTP/1.1, HTTP/2, and HTTP/3: `cd examples/std-middleware && go run .`.

- The `*http.Request` given to a net/http handler has the URL, canonical header map, `Host`, `RemoteAddr`, `TLS`,
  request context, and the `ServeMux` pattern and path values. Like in net/http it may be kept after the handler
  returns; its `Body` may not.
- The `http.ResponseWriter` behaves like net/http's: the header is captured at `WriteHeader`, a missing
  `Content-Type` is sniffed, trailers work through the `Trailer` header or `http.TrailerPrefix`, and
  `http.ErrAbortHandler` aborts quietly. It implements `http.Flusher`, `http.ResponseController`'s `FlushError`,
  and `http.Hijacker` on HTTP/1.x only.
- `qilat.Chain(a, b)(h)` is `a(b(h))`: middleware runs in the order listed. Native middleware is
  `func(qilat.Handler) qilat.Handler`.
- Adapters cost allocations that native handlers avoid: about 0.4 µs and 10 allocations for `FromStd`, and twice
  that for `FromStdMiddleware`, which converts the request both ways.
  See [benchmarks](benchmarks/RESULTS.md#m6-nethttp-adapters-and-compression).

### Static files

`qilat.FileServer` serves an `fs.FS` with the behavior of `http.FileServerFS`, and is tested against it response
for response:

```go
package main

import (
	"log"
	"os"

	"github.com/erizeta/qilat"
)

func main() {
	mux := qilat.NewServeMux()
	mux.Handle("GET /static/", qilat.StripPrefix("/static", qilat.FileServer(os.DirFS("public"))))
	log.Fatal((&qilat.Server{Handler: qilat.Compress(0)(mux)}).ListenAndServe(":8080"))
}
```

```sh
curl -i localhost:8080/static/index.html            # 301 to ./, then serves index.html
curl -H 'Range: bytes=0-99' localhost:8080/static/app.js   # 206 Partial Content
```

[`examples/static`](examples/static/main.go) serves embedded files next to a JSON API: `go run ./examples/static`.

- Range requests (including multipart/byteranges), `If-Modified-Since`, `If-None-Match`, `If-Match`,
  `If-Unmodified-Since`, `If-Range`, `Content-Type` from the extension or by sniffing, `index.html`, and directory
  listings. Paths are cleaned, so requests cannot leave the file system.
- On HTTP/1.1 without TLS, an `*os.File` is sent with sendfile; `io.Copy` to a qilat `ResponseWriter` gets the
  same path.
- `qilat.ServeFile` and `qilat.ServeContent` work like their net/http namesakes; `qilat.Error`, `qilat.NotFound`,
  and `qilat.StripPrefix` too. Unlike net/http, the path left by `StripPrefix` always starts with `/`.

### Compression

`qilat.Compress(level)` gzips responses for clients that accept it. `qilat.CompressWith` sets the minimum size and
adds codings such as zstd or Brotli from any library with a `Reset`-able writer:

```go
package main

import (
	"io"
	"log"

	"github.com/erizeta/qilat"
	"github.com/klauspost/compress/zstd"
)

func main() {
	mux := qilat.NewServeMux()
	mux.HandleFunc("GET /", func(w qilat.ResponseWriter, _ *qilat.Request) {
		w.Header().Set("Content-Type", "text/plain")
		for range 100 {
			_, _ = w.WriteString("compressible text\n")
		}
	})
	zstdEncoder := qilat.Encoder{Name: "zstd", New: func(w io.Writer) qilat.CompressWriter {
		enc, _ := zstd.NewWriter(w, zstd.WithEncoderLevel(zstd.SpeedFastest))
		return enc
	}}
	compress := qilat.CompressWith(qilat.CompressConfig{Encoders: []qilat.Encoder{zstdEncoder}})
	log.Fatal((&qilat.Server{Handler: compress(mux)}).ListenAndServe(":8080"))
}
```

```sh
curl -sI -H 'Accept-Encoding: zstd, gzip' localhost:8080   # Content-Encoding: zstd
curl -sI -H 'Accept-Encoding: gzip' localhost:8080         # Content-Encoding: gzip
```

- The coding is chosen from `Accept-Encoding` q-values; on a tie, extra encoders win over gzip in the order given.
- Skipped: bodies under 1 KiB (`MinSize`), responses that already have `Content-Encoding`, Range requests and
  `206`, `text/event-stream` (so server-sent events still stream), `Cache-Control: no-transform`, and types that do
  not compress, such as images and archives.
- Compressible responses get `Vary: Accept-Encoding`, and a strong `ETag` becomes weak. `Flush` flushes the
  compressor too, so compressed streams arrive as they are written.
- Writers are pooled: compressing allocates nothing per request.

### HTTP client

`qilat.Client` sends requests over HTTP/1.1, HTTP/2, and HTTP/3. Like fasthttp, requests and responses come
from pools, so a request does not allocate; like net/http, cancellation and timeouts follow the request context.

```go
package main

import (
	"fmt"
	"log"
	"time"

	"github.com/erizeta/qilat"
)

func main() {
	client := &qilat.Client{
		Transport: &qilat.Transport{EnableHTTP3: true},
		Timeout:   10 * time.Second,
	}
	req := qilat.AcquireRequest()
	defer qilat.ReleaseRequest(req)
	if err := req.SetRequestURI("https://www.cloudflare.com/"); err != nil {
		log.Fatal(err)
	}
	resp := qilat.AcquireResponse()
	defer qilat.ReleaseResponse(resp)
	for range 2 {
		if err := client.Do(req, resp); err != nil {
			log.Fatal(err)
		}
		body, err := resp.ReadBody()
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(resp.Proto(), resp.StatusCode(), len(body), "bytes")
	}
	// HTTP/2.0 200 ... bytes
	// HTTP/3.0 200 ... bytes  (the first response advertised HTTP/3 with Alt-Svc)
}
```

[`examples/client`](examples/client/main.go) does the same for any URL: `go run ./examples/client https://example.com/`.

- `Transport` keeps HTTP/1.1 connections per host (`MaxConnsPerHost`, `MaxIdleConnsPerHost`, `IdleConnTimeout`)
  and uses HTTP/2 when the server offers it through ALPN, multiplexing requests on one connection; a burst of
  first requests to a new HTTPS host waits for that one connection instead of each opening its own. `H2C: true`
  sends plain `http://` requests over HTTP/2 with prior knowledge, for servers such as a qilat `Server` with `H2C`.
- `DialContext` replaces the dialer for every protocol: it is called with `"tcp"`, and with `"udp"` for HTTP/3.
- With `EnableHTTP3`, HTTPS requests switch to HTTP/3 once the host advertises it with `Alt-Svc` (cached for
  its `ma`). If QUIC cannot connect within `HTTP3DialTimeout`, for example because UDP is blocked, the request
  falls back to TCP and the host stays on TCP for five minutes.
- `resp.Body` streams the body; `resp.ReadBody()` reads all of it into a reused buffer, up to
  `MaxResponseBodySize` (64 MiB by default). Read to EOF or release the response to free the connection.
- Canceling the request context (`req.WithContext(ctx)`) or reaching `Client.Timeout` stops the request even in
  the middle of the body: the HTTP/1.1 connection is closed, and the HTTP/2 or HTTP/3 stream is reset.
- Redirects are followed up to 10 times; `CheckRedirect` can stop earlier, and credentials are dropped when
  the redirect leaves the host. 307 and 308 resend the body when it is an `io.Seeker`.
- Idempotent requests without a body are retried once when the server closed the connection before acting on
  them, such as a stale keep-alive connection or an HTTP/2 stream refused by `GOAWAY`.
- `Proxy` sends HTTPS through an HTTP `CONNECT` tunnel and plain HTTP in absolute form, with
  `Proxy-Authorization` from the proxy URL; `qilat.ProxyFromEnvironment` reads `HTTP_PROXY` and friends.
- HTTP/1.1 throughput matches `fasthttp.Client` and is ahead of `net/http.Client`, with no allocations per
  request. See [benchmarks](benchmarks/RESULTS.md#m7-http11-client).

### Request lifetime

Like fasthttp, qilat reuses requests between clients. A `*Request`, its `Header`, and every `[]byte` it returns
are valid only until `ServeHTTP` returns. Copy what you need, or call `r.Clone(ctx)` to keep the whole request.
`r.Context()` is the exception: it is never reused, so it is safe to pass to goroutines, and like net/http it is
canceled when the client disconnects or the handler returns.

## Roadmap

| Milestone | Scope | Status |
|---|---|---|
| M0 | Repository, CI, baseline benchmarks, dependency research | ✅ Done |
| M1 | Core types: `Header`, `URI`, `Request`, `ResponseWriter`, context, pooling | ✅ Done |
| M2 | HTTP/1.1 server, graceful shutdown | ✅ Done |
| M3 | `ServeMux` with Go 1.22 patterns | ✅ Done |
| M4 | HTTP/2 server | ✅ Done |
| M5 | HTTP/3 server, combined H1/H2/H3 server | ✅ Done |
| M6 | net/http adapters, `FileServer`, compression | ✅ Done |
| M7 | Client and Transport (H1/H2/H3) | ✅ Done |
| M8 | Fuzzing, security review, documentation, v0.1.0 | Planned |

## Contributing

Contributions are welcome. Please read [CONTRIBUTING.md](CONTRIBUTING.md) and the
[Code of Conduct](CODE_OF_CONDUCT.md) first.

## Security

Please do **not** open public issues for vulnerabilities. See [SECURITY.md](SECURITY.md).

## License

[MIT](LICENSE)
