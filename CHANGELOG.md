# Changelog

All notable changes to qilat are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and qilat follows
[Semantic Versioning](https://semver.org/spec/v2.0.0.html): before v1.0.0, minor versions may contain breaking
changes, each listed with what to change.

## [Unreleased]

### Changed

- On HTTP/1.1, a response released or closed with its body unread no longer costs its connection: the rest of
  the body is drained in the background and the connection reused, when it arrives within 50 ms and 256 KiB.
  Before, only a rest already buffered was discarded, and larger or chunked bodies closed the connection.

## [0.2.0] - 2026-11-01

Clients that stop sending a request body or reading a response are now disconnected after 60 s; set both stall
timeouts to a negative value to keep v0.1.0's behavior.

### Added

- `Server.ReadStallTimeout` and `Server.WriteStallTimeout`, with `DefaultStallTimeout` (60 s). They bound how
  long a handler waits for a client that stopped sending the request body or accepting the response, on
  HTTP/1.1, HTTP/2, and HTTP/3, without limiting transfers that keep moving. A stalled read or write fails with
  an error wrapping `os.ErrDeadlineExceeded` and cancels the request context.

### Changed

- A client that stops sending a request body or stops reading a response for 60 s is now disconnected (HTTP/1.1)
  or has its stream reset (HTTP/2, HTTP/3); before, the request waited without limit. To keep the old behavior,
  set `ReadStallTimeout` and `WriteStallTimeout` to a negative value.
- With `WriteStallTimeout` set, HTTP/1.1 and HTTP/3 send long writes in 64 KiB pieces, and HTTP/1.1 sends
  files the same way, so the timeout measures a client that stopped reading rather than the length of a
  transfer. Files are still sent with sendfile.

### Security

- An HTTP/3 request stream had no time limit for its header unless `ReadTimeout` was set, so a client could hold
  up to `HTTP3.MaxConcurrentStreams` streams per connection without sending a request. The header is now bounded
  by `ReadHeaderTimeout`, as on HTTP/1.1.

### Fixed

- On HTTP/1.1, a client disconnecting as its request ended could make the server cancel the context of a later
  request, possibly on another connection, through the pooled exchange (a data race present since v0.1.0).

## [0.1.0] - 2026-10-31

The first release. Minimum Go version: 1.26.

### Added

- Core types: `Request`, `ResponseWriter`, `Header`, `URI`, and `Args`, with `[]byte` accessors that do not
  allocate, pooled requests, and `Request.Clone` to keep a request past its handler.
- `Server` for HTTP/1.1, HTTP/2 (over TLS and cleartext h2c), and HTTP/3 with the same handlers, with
  `ListenAndServe`, `ListenAndServeTLS` (TCP and UDP on one port), `Serve`, `ServeTLS`, `ServeQUIC`,
  `Shutdown`, and `Close`. Zero limits mean safe defaults.
- Per-request contexts that are canceled when the client disconnects, the handler returns, or a `Shutdown`
  deadline passes.
- `Server.DebugPoison`, which makes use of a request or `ResponseWriter` after its handler returns panic.
- `ServeMux` with the pattern syntax, precedence, and redirects of `net/http.ServeMux` (Go 1.22), allocation-free
  matching, `PathValue`, and `PeekPathValue`.
- HTTP/2: flow control tied to handler reads, RFC 9218 priorities, trailers, and protections against Rapid
  Reset, CONTINUATION floods, and control-frame floods. Passes h2spec (147/147).
- HTTP/3 on quic-go with a static-table QPACK decoder, Alt-Svc advertising, optional 0-RTT that serves only
  `GET`, `HEAD`, and `OPTIONS` early and answers other methods `425 Too Early`, and `GOAWAY` on shutdown.
- `FromStd`, `ToStd`, and `FromStdMiddleware` adapters for net/http handlers and middleware, and `Chain` for
  native middleware.
- `FileServer`, `ServeFile`, `ServeContent`, `StripPrefix`, `Error`, and `NotFound` with net/http behavior,
  including Range and conditional requests, and sendfile on HTTP/1.1.
- `Compress` and `CompressWith`: pooled gzip compression with pluggable codings such as zstd and Brotli.
- `Client` and `Transport` for HTTP/1.1, HTTP/2, and HTTP/3 (through Alt-Svc, with fallback to TCP), with
  pooled requests and responses, connection limits, redirects, retries of unprocessed requests, HTTP `CONNECT`
  proxies, `ProxyFromEnvironment`, and `GenerateDevCert` for local TLS.
- Guides: [request lifetime](docs/lifetime.md), [deploying HTTP/3](docs/http3.md), and
  [migrating from net/http and fasthttp](docs/migrating.md).

### Security

Found by the pre-release security and code reviews and fixed before the first release:

- The HTTP/2 client did not limit the decoded size of response header blocks, so a server could make it
  allocate without bound with small references to a large HPACK table entry. Blocks over
  `Transport.MaxResponseHeaderBytes` now fail the request.
- The HTTP/3 client allocated whatever length a server declared for response trailers. Trailers and headers are
  now limited to `MaxResponseHeaderBytes`, encoded and decoded.
- The HTTP/2 server could not enforce `WriteTimeout` against a client that stopped reading, which held the
  connection's writers indefinitely. Socket writes are now bounded by `WriteTimeout` when it is set.
- The HTTP/1.1 client reused connections after responses with both Content-Length and Transfer-Encoding, or
  with bytes left over after the response, which could make a later request read a response that was not its
  own. Such connections are now closed, and only a final coding of `chunked` delimits a body.
- Redirects from HTTPS to HTTP on the same host kept `Authorization` and `Cookie`. They are now dropped.
- `https://` and `socks5://` proxy URLs were dialed as plain HTTP proxies, sending `CONNECT` credentials in
  cleartext. Proxies other than `http://` are now rejected with an error.
- `Response.ReadBody` preallocated the declared Content-Length before data arrived; it now grows with the data.
- The HTTP/1.1 client now stops after 5 informational (1xx) responses, as net/http does.
- One HTTP/2 SETTINGS frame could repeat `INITIAL_WINDOW_SIZE` thousands of times, each a walk over every
  stream; the value is now applied once per frame.

### Fixed

- `Server.ReadTimeout` canceled the request context of HTTP/1.1 handlers that ran longer than the timeout after
  reading the whole body.
- A late reset callback could cancel the context of a later request served by the same pooled HTTP/3 stream.
- A late cancellation of an HTTP/1.1 client request could leave a timed-out connection in the idle pool.
- `Transport.CloseIdleConnections` racing with the idle timer could panic, and stopped idle reaping for
  HTTP/2 and HTTP/3 connections that were busy at the time.

[Unreleased]: https://github.com/erizeta/qilat/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/erizeta/qilat/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/erizeta/qilat/releases/tag/v0.1.0
