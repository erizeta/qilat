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

Every claim above will be backed by reproducible benchmarks in [`benchmarks/`](benchmarks/) before release.

## Requirements

- Go **1.26** or newer. CI tests the two latest Go releases.

## Installation

```sh
go get github.com/erizeta/qilat
```

## Quick start

qilat currently serves **HTTP/1.1** (plain and TLS). HTTP/2 and HTTP/3 follow on the [roadmap](#roadmap).

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

### Request lifetime

Like fasthttp, qilat reuses requests between clients. A `*Request`, its `Header`, and every `[]byte` it returns
are valid only until `ServeHTTP` returns. Copy what you need, or call `r.Clone(ctx)` to keep the whole request.
`r.Context()` is the exception: it is never reused, so it is safe to pass to goroutines, and like net/http it is
canceled when the client disconnects or the handler returns.

## Planned API

The snippet below shows the intended shape of HTTP/2 and HTTP/3 support. **It does not work yet**: today
`ListenAndServeTLS` serves HTTP/1.1 only.

```go
srv := &qilat.Server{Handler: mux}
// Serves HTTP/1.1 and HTTP/2 over TCP and HTTP/3 over UDP on the same port.
log.Fatal(srv.ListenAndServeTLS(":8443", "cert.pem", "key.pem"))
```

## Roadmap

| Milestone | Scope | Status |
|---|---|---|
| M0 | Repository, CI, baseline benchmarks, dependency research | ✅ Done |
| M1 | Core types: `Header`, `URI`, `Request`, `ResponseWriter`, context, pooling | ✅ Done |
| M2 | HTTP/1.1 server, graceful shutdown | ✅ Done |
| M3 | `ServeMux` with Go 1.22 patterns | ✅ Done |
| M4 | HTTP/2 server | Planned |
| M5 | HTTP/3 server, combined H1/H2/H3 server | Planned |
| M6 | net/http adapters, `FileServer`, compression | Planned |
| M7 | Client and Transport (H1/H2/H3) | Planned |
| M8 | Fuzzing, security review, documentation, v0.1.0 | Planned |

## Contributing

Contributions are welcome. Please read [CONTRIBUTING.md](CONTRIBUTING.md) and the
[Code of Conduct](CODE_OF_CONDUCT.md) first.

## Security

Please do **not** open public issues for vulnerabilities. See [SECURITY.md](SECURITY.md).

## License

[MIT](LICENSE)
