# qilat

[![Test](https://github.com/erizeta/qilat/actions/workflows/test.yml/badge.svg)](https://github.com/erizeta/qilat/actions/workflows/test.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/erizeta/qilat.svg)](https://pkg.go.dev/github.com/erizeta/qilat)
[![Go Report Card](https://goreportcard.com/badge/github.com/erizeta/qilat)](https://goreportcard.com/report/github.com/erizeta/qilat)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

High-performance HTTP library for Go with **net/http-style APIs** and native **HTTP/1.1, HTTP/2, and HTTP/3**
support, for servers and clients.

> [!NOTE]
> qilat is pre-1.0. The API may still change between minor versions; every change is listed in the
> [changelog](CHANGELOG.md). See [API stability](#api-stability).

## Why qilat?

[fasthttp](https://github.com/valyala/fasthttp) is fast thanks to zero-allocation parsing and aggressive
pooling, but it only speaks HTTP/1.1, cancels requests only on shutdown, and uses its own API that does not fit
the net/http ecosystem. [net/http](https://pkg.go.dev/net/http) supports HTTP/2 and has a familiar API, but
allocates more per request and has no HTTP/3.

qilat takes net/http's API shape and fasthttp's performance model, and adds HTTP/3:

| | net/http | fasthttp | qilat |
|---|---|---|---|
| HTTP/1.1 | ✅ | ✅ | ✅ |
| HTTP/2 | ✅ | ❌ | ✅ native |
| HTTP/3 (QUIC) | ❌ | ❌ | ✅ native |
| net/http-style `Handler` / `ResponseWriter` | ✅ | ❌ | ✅ |
| `ServeMux` with Go 1.22 patterns (`GET /users/{id}`) | ✅ | ❌ | ✅ allocation-free |
| Per-request `context.Context` cancellation | ✅ | ❌ (shutdown only) | ✅ |
| Zero-allocation hot path | ❌ | ✅ | ✅ |
| Streaming request/response bodies | ✅ | partial | ✅ |
| Adapters to and from `net/http` | — | partial | ✅ |
| `FileServer` with Range, conditional requests, and sendfile | ✅ | ✅ | ✅ |
| Response compression (gzip, pluggable zstd/br) | ❌ | ✅ | ✅ allocation-free |
| HTTP client | ✅ H1, H2 | ✅ H1 | ✅ H1, H2, H3, allocation-free |

Performance claims are backed by reproducible [benchmarks](#performance).

## Contents

- [Installation](#installation) · [Quick start](#quick-start)
- Server: [handling requests](#handling-requests) · [streaming and cancellation](#streaming-and-cancellation) ·
  [graceful shutdown](#graceful-shutdown) · [middleware](#middleware) · [routing](#routing) · [HTTP/2](#http2) ·
  [HTTP/3](#http3) · [net/http handlers](#nethttp-handlers-and-middleware) · [static files](#static-files) ·
  [compression](#compression)
- [HTTP client](#http-client)
- [Request lifetime](#request-lifetime) · [Configuration](#configuration) · [Performance](#performance) ·
  [Security](#security) · [API stability](#api-stability)
- Guides: [request lifetime](docs/lifetime.md) · [deploying HTTP/3](docs/http3.md) ·
  [migrating from net/http and fasthttp](docs/migrating.md)

## Installation

```sh
go get github.com/erizeta/qilat
```

qilat needs Go **1.26** or newer. CI tests the two latest Go releases on Linux and macOS.

## Quick start

```go
package main

import (
	"log"

	"github.com/erizeta/qilat"
)

func main() {
	h := qilat.HandlerFunc(func(w qilat.ResponseWriter, r *qilat.Request) {
		_, _ = w.WriteString("Hello over " + r.Proto() + "!\n")
	})
	log.Fatal((&qilat.Server{Handler: h}).ListenAndServe(":8080"))
}
```

```sh
curl localhost:8080   # Hello over HTTP/1.1!
```

`ListenAndServeTLS` serves HTTP/1.1 and HTTP/2 over TCP and HTTP/3 over UDP, on the same port and with the same
handlers. For local development, `qilat.GenerateDevCert` makes a self-signed certificate:

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
curl -k --http1.1 https://localhost:8443      # hello over HTTP/1.1
curl -k https://localhost:8443                # hello over HTTP/2.0, with alt-svc: h3=":8443"; ma=86400
curl -k --http3-only https://localhost:8443   # hello over HTTP/3.0 (needs a curl built with HTTP/3)
```

In production, pass your certificate files instead: `srv.ListenAndServeTLS(":443", "cert.pem", "key.pem")`.
[`examples/`](examples/) has runnable programs, such as [`examples/hello`](examples/hello/main.go)
(`go run ./examples/hello`) and [`examples/h3-dev-cert`](examples/h3-dev-cert/main.go).

## Handling requests

Headers, query parameters, and the body work as in net/http, with `[]byte` variants that do not allocate:

```go
package main

import (
	"encoding/json"
	"log"
	"net/http"

	"github.com/erizeta/qilat"
)

func main() {
	mux := qilat.NewServeMux()
	mux.HandleFunc("POST /greet", func(w qilat.ResponseWriter, r *qilat.Request) {
		var in struct {
			Name string `json:"name"`
		}
		if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
			qilat.Error(w, "invalid JSON", http.StatusBadRequest)
			return
		}
		lang := r.URI().Query().Get("lang")  // a string copy
		agent := r.Header.Peek("User-Agent") // bytes valid until the handler returns
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{
			"message": "hello " + in.Name,
			"lang":    lang,
			"agent":   string(agent),
		})
	})
	log.Fatal((&qilat.Server{Handler: mux}).ListenAndServe(":8080"))
}
```

```sh
curl -d '{"name":"qilat"}' 'localhost:8080/greet?lang=en'
# {"agent":"curl/8.7.1","lang":"en","message":"hello qilat"}
```

- `r.Method()`, `r.Path()`, `r.Host()`, `r.Proto()`, `r.RemoteAddr()`, `r.TLS()`, and `r.ContentLength()`
  describe the request; `r.URI()` gives the decoded path, raw query, and parsed `Args`.
- `r.Header.Get` and `Args.Get` return strings; `Peek` returns bytes without allocating, and `PeekAll` every
  value of a repeated field. Names are case-insensitive.
- `r.Body` is an `io.Reader` that is never nil. Bodies over `MaxRequestBodySize` (4 MiB by default) get `413`,
  or `qilat.ErrBodyTooLarge` from `Read` when the length was not declared.
- The keep-alive hot path of the HTTP/1.1 server allocates nothing per request.

`Header`, `URI`, and `Args` are usable on their own, for example to parse a URL-encoded form body with
`Args.Parse`. [`examples/core`](examples/core/main.go) shows them: `go run ./examples/core`.

## Streaming and cancellation

`ResponseWriter` is an `io.Writer` with `Flush`, so a handler streams by writing and flushing as it goes.
`r.Context()` is canceled when the client disconnects (an HTTP/1.1 connection closes, an HTTP/2 or HTTP/3 stream
is reset) and when the handler returns, so long-running work can stop early:

```go
package main

import (
	"fmt"
	"log"
	"time"

	"github.com/erizeta/qilat"
)

func main() {
	mux := qilat.NewServeMux()
	mux.HandleFunc("GET /events", func(w qilat.ResponseWriter, r *qilat.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		tick := time.NewTicker(time.Second)
		defer tick.Stop()
		for i := 1; ; i++ {
			select {
			case <-r.Context().Done(): // the client went away
				return
			case <-tick.C:
				_, _ = fmt.Fprintf(w, "data: tick %d\n\n", i)
				if err := w.Flush(); err != nil {
					return
				}
			}
		}
	})
	log.Fatal((&qilat.Server{Handler: mux}).ListenAndServe(":8080"))
}
```

```sh
curl -N localhost:8080/events   # data: tick 1, data: tick 2, ... one per second
```

Pass `r.Context()` to database calls and outgoing requests so they stop with the request. Unlike the request
itself, the context is never reused, so it is safe to hand to goroutines.

## Graceful shutdown

`Shutdown` stops accepting connections, closes idle ones, sends `GOAWAY` on HTTP/2 and HTTP/3, and waits for
running requests. If its context ends first, the contexts of running requests are canceled. `Close` stops
everything at once.

```go
package main

import (
	"context"
	"errors"
	"log"
	"os/signal"
	"syscall"
	"time"

	"github.com/erizeta/qilat"
)

func main() {
	srv := &qilat.Server{Handler: qilat.HandlerFunc(func(w qilat.ResponseWriter, _ *qilat.Request) {
		_, _ = w.WriteString("ok\n")
	})}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	drained := make(chan struct{})
	go func() {
		defer close(drained)
		<-ctx.Done()
		timeout, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := srv.Shutdown(timeout); err != nil {
			log.Print("shutdown: ", err)
		}
	}()

	if err := srv.ListenAndServe(":8080"); !errors.Is(err, qilat.ErrServerClosed) {
		log.Fatal(err)
	}
	<-drained // ListenAndServe returns as soon as Shutdown starts; wait for running requests
}
```

## Middleware

Native middleware is a `func(qilat.Handler) qilat.Handler`, and `qilat.Chain(a, b)(h)` is `a(b(h))`, so
middleware runs in the order listed:

```go
package main

import (
	"log"
	"net/http"
	"time"

	"github.com/erizeta/qilat"
)

func logging(next qilat.Handler) qilat.Handler {
	return qilat.HandlerFunc(func(w qilat.ResponseWriter, r *qilat.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		log.Printf("%s %s %v", r.Method(), r.Path(), time.Since(start))
	})
}

func requireToken(next qilat.Handler) qilat.Handler {
	return qilat.HandlerFunc(func(w qilat.ResponseWriter, r *qilat.Request) {
		if string(r.Header.Peek("Authorization")) != "Bearer secret" {
			qilat.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func main() {
	mux := qilat.NewServeMux()
	mux.HandleFunc("GET /", func(w qilat.ResponseWriter, _ *qilat.Request) {
		_, _ = w.WriteString("welcome\n")
	})
	h := qilat.Chain(logging, requireToken, qilat.Compress(0))(mux)
	log.Fatal((&qilat.Server{Handler: h}).ListenAndServe(":8080"))
}
```

net/http middleware works too, through [`qilat.FromStdMiddleware`](#nethttp-handlers-and-middleware).

Handler panics are recovered and logged, and the response is aborted, as in net/http; panic with
`qilat.ErrAbortHandler` to abort without logging.

## Routing

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

## HTTP/2

HTTP/2 needs no extra code. `ListenAndServeTLS` offers `h2` through ALPN and falls back to HTTP/1.1. Set
`H2C: true` to also accept cleartext HTTP/2 (prior knowledge) on a plain listener:

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

- Passes the [h2spec](https://github.com/summerwind/h2spec) conformance suite (147/147 in strict mode over
  TLS), run in CI.
- Flow control follows how fast handlers read request bodies, so a slow handler slows its client down instead
  of buffering without bound.
- Responses are scheduled by RFC 9218 priority (`priority` header and `PRIORITY_UPDATE`).
- Protections against Rapid Reset (CVE-2023-44487), CONTINUATION floods, and control-frame floods are always on.
- `Server.HTTP2` tunes limits; zero values are safe defaults. `DisableHTTP2` serves HTTP/1.1 only.

Trailers work on HTTP/1.1 too, in a chunked body. They are dropped only when the handler sets `Content-Length`
itself, or for HTTP/1.0 clients. [`examples/http2`](examples/http2/main.go) serves HTTPS and h2c side by side.

## HTTP/3

`ListenAndServeTLS` serves HTTP/3 over UDP on the same port as TCP (see the [quick start](#quick-start)), and
adds an `Alt-Svc` header to HTTP/1.1 and HTTP/2 responses so browsers and curl switch to HTTP/3.

- If either the TCP or the UDP listener fails, `ListenAndServeTLS` closes the other and returns the error.
- `ServeQUIC(packetConn)` serves HTTP/3 on a UDP socket of your own; `DisableHTTP3` keeps `ListenAndServeTLS`
  on TCP only.
- `Server.HTTP3` tunes stream and flow-control limits and the Alt-Svc max age, or turns Alt-Svc off.
- 0-RTT is off by default. With `HTTP3.Allow0RTT`, returning clients save a round trip, and early requests
  other than `GET`, `HEAD`, and `OPTIONS` get `425 Too Early`, since 0-RTT data can be replayed.
- QPACK uses only the static table, so it needs no extra streams and decodes requests without allocating.

QUIC runs over UDP, so firewalls and load balancers need to let it through. [Deploying HTTP/3](docs/http3.md)
covers ports, load balancers, TLS, Linux UDP buffer sizes, and 0-RTT.

## net/http handlers and middleware

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
or cors) whose test runs this chain over HTTP/1.1, HTTP/2, and HTTP/3.

- The `*http.Request` given to a net/http handler has the URL, canonical header map, `Host`, `RemoteAddr`, `TLS`,
  request context, and the `ServeMux` pattern and path values. Like in net/http it may be kept after the handler
  returns; its `Body` may not.
- The `http.ResponseWriter` behaves like net/http's: the header is captured at `WriteHeader`, a missing
  `Content-Type` is sniffed, trailers work through the `Trailer` header or `http.TrailerPrefix`, and
  `http.ErrAbortHandler` aborts quietly. It implements `http.Flusher`, `http.ResponseController`'s `FlushError`,
  and `http.Hijacker` on HTTP/1.x only.
- Adapters cost allocations that native handlers avoid: about 0.5 µs and 10 allocations for `FromStd`, and twice
  that for `FromStdMiddleware`, which converts the request both ways.

Moving an existing application over gradually is covered in [migrating from net/http](docs/migrating.md).

## Static files

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

- Range requests (including multipart/byteranges), `If-Modified-Since`, `If-None-Match`, `If-Match`,
  `If-Unmodified-Since`, `If-Range`, `Content-Type` from the extension or by sniffing, `index.html`, and directory
  listings. Paths are cleaned, so requests cannot leave the file system.
- On HTTP/1.1 without TLS, an `*os.File` is sent with sendfile; `io.Copy` to a qilat `ResponseWriter` gets the
  same path.
- `qilat.ServeFile` and `qilat.ServeContent` work like their net/http namesakes; `qilat.Error`, `qilat.NotFound`,
  and `qilat.StripPrefix` too. Unlike net/http, the path left by `StripPrefix` always starts with `/`.

[`examples/static`](examples/static/main.go) serves embedded files next to a JSON API: `go run ./examples/static`.

## Compression

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

## HTTP client

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
  Response headers are limited by `MaxResponseHeaderBytes` (64 KiB by default) on every protocol.
- `client.Do(req.WithContext(ctx), resp)` or `Client.Timeout` stops the request even in the middle of the body:
  the HTTP/1.1 connection is closed, and the HTTP/2 or HTTP/3 stream is reset.
- Redirects are followed up to 10 times; `CheckRedirect` can stop earlier. Credentials (`Authorization`,
  `Cookie`) are dropped when a redirect leaves the host or goes from HTTPS to HTTP. 307 and 308 resend the body
  when it is an `io.Seeker`.
- Idempotent requests without a body are retried once when the server closed the connection before acting on
  them, such as a stale keep-alive connection or an HTTP/2 stream refused by `GOAWAY`.
- `Proxy` sends HTTPS through an HTTP `CONNECT` tunnel and plain HTTP in absolute form, with
  `Proxy-Authorization` from the proxy URL; `qilat.ProxyFromEnvironment` reads `HTTP_PROXY` and friends. Only
  `http://` proxies are supported.

## Request lifetime

Like fasthttp, qilat reuses requests between clients. A `*Request`, its `Header`, its `Body`, the
`ResponseWriter`, and every `[]byte` they return are valid only until `ServeHTTP` returns. Copy what you need,
or call `r.Clone(ctx)` to keep the whole request. `r.Context()` is the exception: it is never reused.

Set `Server.DebugPoison` in tests to make any use after return panic. [docs/lifetime.md](docs/lifetime.md) has the
full rules, with examples of correct and incorrect use, and the matching rules for client requests and responses.

## Configuration

Unlike net/http, a zero value means a safe limit, not "unlimited". Set a field to a negative value to disable it.

| `Server` field | Zero value means |
|---|---|
| `ReadHeaderTimeout` | 10 s, also used for the TLS and QUIC handshakes |
| `IdleTimeout` | 120 s between keep-alive requests, and the QUIC idle timeout |
| `MaxHeaderBytes` | 64 KiB of request line and header (larger headers get 431) |
| `MaxRequestBodySize` | 4 MiB (larger bodies get 413, or `qilat.ErrBodyTooLarge` while reading) |
| `ReadTimeout` | no limit, so slow uploads work |
| `WriteTimeout` | no limit, so streaming responses work |
| `MaxConcurrentConns` | no limit |
| `HTTP2.MaxConcurrentStreams`, `HTTP3.MaxConcurrentStreams` | 250 concurrent requests per connection |

`ReadTimeout` bounds reading the request, body included, and `WriteTimeout` writing the response. Both are off
by default because a fixed limit breaks large uploads and long streams; set them when neither is expected.
Behind a reverse proxy that enforces its own timeouts, the defaults are usually enough.

Other settings:

- `TLSConfig` is cloned, never modified. `H2C`, `DisableHTTP2`, and `DisableHTTP3` choose protocols.
- `HTTP2` and `HTTP3` tune per-protocol limits: receive buffers, frame size, and Alt-Svc.
- `BaseContext` sets the parent context of requests; `Logger` takes a `*slog.Logger` for handler panics and
  connection errors.
- `ReduceMemoryUsage` returns read buffers to a pool while connections are idle, for servers with many idle
  connections.

## Performance

Measured on an Apple M5 (10 cores), macOS 26.4, Go 1.27.3, medians of 5 runs. Full results, raw output, and how
to reproduce them are in [benchmarks/RESULTS.md](benchmarks/RESULTS.md).

| Benchmark | net/http | fasthttp | qilat |
|---|---|---|---|
| HTTP/1.1 server, 13-byte response, in-memory | 964 ns, 22 allocs | 247 ns, 0 allocs | **235 ns, 0 allocs** |
| HTTP/1.1 server, 20 request headers, in-memory | 1.87 µs, 64 allocs | **357 ns**, 0 allocs | 414 ns, 0 allocs |
| HTTP/1.1 server, 16 pipelined requests per write | 2.04 µs | 379 ns | **343 ns** |
| HTTP/2 server, h2load, 8 connections × 100 streams | 362k req/s | — | **895k req/s** |
| HTTP/1.1 client, 20 response headers | 7.26 µs, 78 allocs | 5.70 µs, 0 allocs | **5.74 µs, 0 allocs** |

| Benchmark | quic-go/http3 | qilat |
|---|---|---|
| HTTP/3 server, 13-byte response, quic-go client | 16.0 µs, 199 allocs | **11.3 µs, 131 allocs** |
| HTTP/3 server, 20 request headers | 25.0 µs, 341 allocs | **14.4 µs, 189 allocs** |

The server numbers measure server cost: HTTP/1.1 uses an in-memory listener and a minimal client, so neither the
kernel nor the client hides the difference. Over real TCP on this machine, all three HTTP/1.1 servers are
within 10% of each other because the kernel dominates. Reproduce with:

```sh
cd benchmarks && go test -run '^$' -bench . -benchmem -count 5 ./...
```

## Security

- The HTTP/1.1 parser follows RFC 9112 strictly and rejects request smuggling patterns (Content-Length with
  Transfer-Encoding, duplicate lengths, obsolete line folding, bare LF) with the right status code.
- Header values are sanitized against injection; invalid header names panic instead of reaching the wire.
- HTTP/2 and HTTP/3 limit streams, header sizes, and control frames per connection by default.
- Parsers for HTTP/1.1 requests and responses, HTTP/2 frames and HPACK, HTTP/3 frames, control streams, and
  QPACK, URIs, query strings, and headers are fuzzed nightly in CI.
- `FileServer` cleans paths, so requests cannot leave the file system it serves.

Please report vulnerabilities privately, as described in [SECURITY.md](SECURITY.md), not in public issues.

## API stability

qilat follows [Semantic Versioning](https://semver.org/). Before v1.0.0, minor versions (v0.**x**.0) may contain
breaking changes, each listed in the [changelog](CHANGELOG.md) with what to change; patch versions only fix bugs.
Packages under `internal/` are not part of the API. The minimum Go version is raised only when a dependency or
standard library feature needs it, and that is also noted in the changelog.

Known gaps, planned after v0.1.0: cookie and form helpers (`r.Cookie`, `r.FormValue`) and multipart forms, which
work today through `FromStd`; proxies other than `http://` in the client.

## Contributing

Contributions are welcome. Please read [CONTRIBUTING.md](CONTRIBUTING.md) and the
[Code of Conduct](CODE_OF_CONDUCT.md) first.

## License

[MIT](LICENSE). Parts derived from the Go standard library carry its license in [LICENSE-GO](LICENSE-GO).
