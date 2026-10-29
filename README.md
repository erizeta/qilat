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

## Planned API

The snippet below shows the intended shape of the API. **It does not compile yet.**

```go
mux := qilat.NewServeMux()
mux.HandleFunc("GET /users/{id}", func(w qilat.ResponseWriter, r *qilat.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(`{"id":"` + r.PathValue("id") + `"}`))
})

srv := &qilat.Server{Handler: mux}
// Serves HTTP/1.1 and HTTP/2 over TCP and HTTP/3 over UDP on the same port.
log.Fatal(srv.ListenAndServeTLS(":8443", "cert.pem", "key.pem"))
```

## Roadmap

| Milestone | Scope | Status |
|---|---|---|
| M0 | Repository, CI, baseline benchmarks, dependency research | 🚧 In progress |
| M1 | Core types: `Header`, `URI`, `Request`, `ResponseWriter`, context, pooling | Planned |
| M2 | HTTP/1.1 server, graceful shutdown | Planned |
| M3 | `ServeMux` with Go 1.22 patterns | Planned |
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
