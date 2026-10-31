# Benchmark results

Comparison benchmarks for qilat against net/http and fasthttp. The latest results come first;
the **M0 baseline** (net/http and fasthttp only) is kept at the end for reference.

## How to run

```sh
cd benchmarks
go test -run '^$' -bench . -benchmem -benchtime 1s -count 10 ./... > new.txt
go run golang.org/x/perf/cmd/benchstat@latest results/m2.txt new.txt
```

Or from the repository root: `make bench BENCHCOUNT=10`.

Flags:

- `-parallelism N` (default 8): client connections per `GOMAXPROCS`.

## Scenarios

| Scenario | Request | Response |
|---|---|---|
| `plaintext` | `GET /plaintext` | 13 bytes |
| `json-1kb` | `GET /json` | ~1 KB pre-encoded JSON (measures the HTTP stack, not a JSON encoder) |
| `headers-20` | `GET /headers` with 20 request headers; the handler reads one | the header value |
| `upload-64kb` | `POST /upload` with a 64 KB body; the handler reads all of it | body length |

## Modes

| Mode | Transport | What it shows |
|---|---|---|
| `keepalive` | TCP loopback, one request per write | Typical persistent connections |
| `newconn` | TCP loopback, new connection per request (closed with RST to avoid TIME_WAIT exhaustion) | Connection setup cost |
| `pipeline16` | TCP loopback, 16 pipelined requests per write | Server cost with fewer syscalls per request |
| `inmem` | `fasthttputil.InmemoryListener`, no kernel | **Server CPU and allocation cost**, the number qilat optimizes |
| H2C | Cleartext HTTP/2 with prior knowledge, net/http client | HTTP/2 protocol cost without TLS |

HTTP/1.1 modes use a minimal shared client (`h1client_test.go`) so the client adds almost no cost.
H2C uses the net/http client, so its allocations are included in the H2C numbers.
`allocs/op` is process-wide, so it includes the client.

## v0.2.0: stall timeouts

The same comparisons as M8 after adding `ReadStallTimeout` and `WriteStallTimeout`, which are on by default. Same
machine, Go, and dependency versions as M8, `-benchtime 1s -count 5`, medians. Raw output:
[`v0.2.0-h1.txt`](results/v0.2.0-h1.txt), [`v0.2.0-h2c.txt`](results/v0.2.0-h2c.txt),
[`v0.2.0-h3.txt`](results/v0.2.0-h3.txt), [`v0.2.0-client.txt`](results/v0.2.0-client.txt).
The net/http, fasthttp, and quic-go results moved between −7% and +3% from M8, so the machine was in the same
state.

### HTTP/1.1 server, time per request (lower is better)

| Scenario | Mode | net/http | fasthttp | qilat | qilat vs M8 | qilat allocs |
|---|---|---|---|---|---|---|
| plaintext | inmem | 934 ns | 241 ns | 242 ns | +2.8% | 0 |
| json-1kb | inmem | 969 ns | 252 ns | 261 ns | +6.2% | 0 |
| headers-20 | inmem | 1.92 µs | **352 ns** | 421 ns | +1.6% | 0 |
| upload-64kb | inmem | 1.27 µs | 897 ns | **769 ns** | +3.0% | 1 (handler) |
| plaintext | pipeline16 | 2.00 µs | 373 ns | **338 ns** | −1.4% | 0 |
| json-1kb | pipeline16 | 1.94 µs | 760 ns | **696 ns** | −1.8% | 0 |
| headers-20 | pipeline16 | 2.87 µs | 662 ns | **517 ns** | −0.1% | 0 |

### HTTP/2 (H2C, net/http client) and HTTP/3 (quic-go client)

| Scenario | net/http H2C | qilat H2C | quic-go/http3 | qilat H3 |
|---|---|---|---|---|
| plaintext | 12.6 µs | **7.98 µs** | 15.0 µs | **6.65 µs** ³ |
| json-1kb | 15.2 µs | **10.2 µs** | 19.5 µs | **7.49 µs** ³ |
| headers-20 | 22.1 µs | **13.5 µs** | 24.0 µs | **8.57 µs** ³ |
| upload-64kb | 49.7 µs | **27.2 µs** | 240 µs | 230 µs |

Allocations are unchanged from M8 within one per request: H2C 53–106 (qilat), H3 128–759 (qilat).

### HTTP/1.1 client

| Scenario | net/http | fasthttp | qilat | allocs (net/http / fasthttp / qilat) |
|---|---|---|---|---|
| plaintext | 6.03 µs | **5.69 µs** | 5.77 µs | 48 / 0 / 0 |
| json-1kb | 6.22 µs | **5.70 µs** | 5.78 µs | 48 / 0 / 0 |
| headers-20 | 7.10 µs | 5.74 µs | **5.67 µs** | 78 / 0 / 0 |
| upload-64kb | 12.4 µs | **9.14 µs** | 9.16 µs | 58 / 1 / 1 |

### Observations

- The stall timeouts add no allocations. Their cost is a clock read per socket read of a request body on
  HTTP/1.1, and a timer update when a stall deadline is due for renewal. Interleaved runs against v0.1.0 on this
  machine put it at about +7% for `upload-64kb` in `inmem` and within noise elsewhere; the table above, taken
  against M8, shows +1.6% to +6.2% in `inmem`. Setting both timeouts to a negative value removes it.
- ³ qilat's small HTTP/3 requests take about 40% less time than in M8, and 3 fewer allocations, but not because
  the server does less work. Bypassing the write stall watchdog brings the numbers back to v0.1.0's (10.4 µs for
  plaintext measured the same day), although the watchdog only adds work. The likely cause is that each pooled
  stream keeps a 60-second timer armed, which changes how the Go scheduler wakes goroutines in this in-process,
  loopback benchmark. Do not read it as a speedup; M8's HTTP/3 numbers remain the reference.

## M8: final comparison (v0.1.0)

Rerun of every comparison after the M8 hardening changes, on the same machine as M2 (Apple M5, 10 cores,
macOS 26.4, Go 1.27.3, fasthttp v1.77.0, quic-go v0.64.1), `-benchtime 1s -count 5`, medians. Raw output:
[`m8-h1.txt`](results/m8-h1.txt), [`m8-h2c.txt`](results/m8-h2c.txt), [`m8-h3.txt`](results/m8-h3.txt),
[`m8-client.txt`](results/m8-client.txt).

```sh
cd benchmarks
go test -run '^$' -bench 'BenchmarkH1$/.*/.*/(inmem|pipeline16)' -benchmem -count 5 .
go test -run '^$' -bench 'BenchmarkH2C$|BenchmarkH3$|BenchmarkClientH1$' -benchmem -count 5 .
```

### HTTP/1.1 server, time per request (lower is better)

| Scenario | Mode | net/http | fasthttp | qilat | qilat allocs |
|---|---|---|---|---|---|
| plaintext | inmem | 964 ns | 247 ns | **235 ns** | 0 |
| json-1kb | inmem | 972 ns | 254 ns | **246 ns** | 0 |
| headers-20 | inmem | 1.87 µs | **357 ns** | 414 ns | 0 |
| upload-64kb | inmem | 1.16 µs | 881 ns | **747 ns** | 1 (handler) |
| plaintext | pipeline16 | 2.04 µs | 379 ns | **343 ns** | 0 |
| json-1kb | pipeline16 | 1.99 µs | 773 ns | **708 ns** | 0 |
| headers-20 | pipeline16 | 2.96 µs | 686 ns | **517 ns** | 0 |

### HTTP/2 (H2C, net/http client) and HTTP/3 (quic-go client)

| Scenario | net/http H2C | qilat H2C | quic-go/http3 | qilat H3 |
|---|---|---|---|---|
| plaintext | 12.4 µs | **8.1 µs** | 16.0 µs | **11.3 µs** |
| json-1kb | 15.6 µs | **11.0 µs** | 19.8 µs | **12.1 µs** |
| headers-20 | 21.5 µs | **13.9 µs** | 25.0 µs | **14.4 µs** |
| upload-64kb | 48.2 µs | **28.8 µs** | 259 µs | 254 µs |

Allocations include the client: H2C 86–169 (net/http) against 53–106 (qilat), H3 199–883 (quic-go/http3)
against 131–769 (qilat).

### HTTP/1.1 client

| Scenario | net/http | fasthttp | qilat | allocs (net/http / fasthttp / qilat) |
|---|---|---|---|---|
| plaintext | 6.15 µs | **5.64 µs** | 5.75 µs | 48 / 0 / 0 |
| json-1kb | 6.09 µs | 5.77 µs | **5.71 µs** | 48 / 0 / 0 |
| headers-20 | 7.26 µs | **5.70 µs** | 5.74 µs | 78 / 0 / 0 |
| upload-64kb | 13.0 µs | **9.23 µs** | 9.32 µs | 58 / 1 / 1 |

### Observations

- Numbers match the per-milestone results below within noise: the M8 fixes (response header limits in the
  clients, write deadlines for HTTP/2 `WriteTimeout`, stream cancellation bookkeeping in HTTP/3) are off the
  hot path or cost no allocations.
- `headers-20` in `inmem` is the one server scenario where fasthttp stays ahead (~16%): qilat validates,
  lowercases, and copies each header field.
- The h2load results in [M4](#m4-http2-server) were not rerun; the HTTP/2 server code they measured changed only
  in SETTINGS handling and in the `WriteTimeout` path, which h2load does not use.

## M5: HTTP/3 server

`BenchmarkH3` serves the same scenarios with qilat and the quic-go/http3 server, both on quic-go v0.64.1, and
drives them with the quic-go/http3 client over loopback UDP. Same machine and Go version as M2; medians over 5
runs, raw output in [`results/m5-h3.txt`](results/m5-h3.txt). The client and QUIC itself are most of the cost.

| Scenario | quic-go/http3 | qilat | quic-go/http3 allocs | qilat allocs |
|---|---|---|---|---|
| plaintext | 15.3 µs | **10.5 µs** | 199 | 131 |
| json-1kb | 19.5 µs | **11.9 µs** | 206 | 139 |
| headers-20 | 24.1 µs | **14.0 µs** | 341 | 190 |
| upload-64kb | 245 µs | 242 µs | 880 | 769 |

- qilat decodes QPACK into the pooled request without allocating (static table only) and writes each response
  in one stream write; quic-go/http3 builds a `net/http` request per stream. The upload is bound by QUIC flow
  control and packet handling, which both servers share.
- No out-of-process HTTP/3 load generator is used yet; `h2load --h3` needs an nghttp2 build with ngtcp2.

## M4: HTTP/2 server

Same machine and Go version as M2. Both servers use their default settings; the handler returns 13 bytes.

### Out-of-process load: h2load, cleartext HTTP/2, requests per second (higher is better)

`go run ./cmd/h2loadserver`, then `h2load -n 1000000 <flags> http://127.0.0.1:19001/` (net/http) or `:19002`
(qilat). Medians of 3 runs; raw output in [`results/m4-h2load.txt`](results/m4-h2load.txt).

| Load | net/http | qilat |
|---|---|---|
| 8 connections × 100 concurrent streams (`-c 8 -m 100 -t 4`) | 362k | **895k** (2.5×) |
| 1 connection × 100 concurrent streams (`-c 1 -m 100 -t 1`) | 110k | **371k** (3.4×) |

Bytes on the wire are the same for both: qilat's HPACK encoder indexes repeated response fields like net/http's,
so headers cost ~4 bytes per response once the dynamic table is warm.

### In-process: `BenchmarkH2C`, net/http client, time per request

Medians over 5 runs, raw output in [`results/m4-h2c.txt`](results/m4-h2c.txt). The net/http client is most of the
cost and most of the allocations here, so the gap is smaller than with h2load.

| Scenario | net/http | qilat | net/http allocs | qilat allocs |
|---|---|---|---|---|
| plaintext | 12.2 µs | **8.6 µs** | 86 | 53 |
| json-1kb | 15.2 µs | **10.8 µs** | 89 | 57 |
| headers-20 | 22.2 µs | **14.0 µs** | 169 | 106 |
| upload-64kb | 62.3 µs | **30.0 µs** | 132 | 73 |

### Observations

- qilat writes frames directly from handler goroutines under a priority-ordered write lock instead of handing
  every frame to a connection goroutine, and the last writer in line flushes for everyone. Most of the gain on
  a single connection comes from that.
- Per request the HTTP/2 server allocates the stream, its handler goroutine, and HPACK strings for request
  fields that are not in the client's dynamic table; responses are encoded without allocating.
- Conformance: h2spec 147/147 in strict mode over TLS (run in CI by `TestH2Spec`).

## M7: HTTP/1.1 client

- `BenchmarkClientH1`: `qilat.Client`, `net/http.Client`, and `fasthttp.Client` send the same requests to the
  same fasthttp server over loopback with keep-alive, `-parallelism 8`, each with an idle pool large enough
  for every goroutine. Raw output in [`results/m7-client.txt`](results/m7-client.txt).
- Same machine and Go version as M2, `-count 10`, medians. Time per request, lower is better.

```sh
cd benchmarks && go test -run '^$' -bench 'ClientH1' -benchmem -count 10 .
```

| Scenario | net/http | fasthttp | qilat | net/http allocs | fasthttp allocs | qilat allocs |
|---|---|---|---|---|---|---|
| plaintext | 6.14 µs | 5.73 µs | **5.75 µs** | 48 | 0 | 0 |
| json-1kb | 5.99 µs | 5.79 µs | **5.79 µs** | 48 | 0 | 0 |
| headers-20 | 7.38 µs | 5.74 µs | **5.70 µs** | 78 | 0 | 0 |
| upload-64kb | 12.89 µs | 9.29 µs | **9.29 µs** | 58 | 1 | 1 |

- The GET scenarios are bound by the loopback round trip, where the three clients differ by less than 7%;
  allocations and the upload show the client's own cost.
- qilat sends a `bytes.Reader` or `strings.Reader` body in one write and reuses the request, response, and
  connection buffers, so a request does not allocate.

## M6: net/http adapters and compression

- Benchmarks in the root package (not this module): one request with three header fields runs through a pooled
  exchange into a no-op sink, so only the core and the adapter are measured. Raw output in
  [`results/m6-adapter.txt`](results/m6-adapter.txt).
- Same machine and Go version as M2, `-count 10`, medians.

```sh
go test -run '^$' -bench 'Adapter|Compress' -benchmem -count 10 .
```

| Handler | Time | B/op | allocs/op |
|---|---|---|---|
| Native qilat handler | 140 ns | 0 | 0 |
| `FromStd(http.Handler)` | 561 ns | 1081 | 10 |
| `FromStdMiddleware(pass-through)` around a qilat handler | 944 ns | 1113 | 13 |
| `ToStd(Handler)` on a minimal net/http writer | 263 ns | 64 | 5 |
| `Compress`, 13-byte body (too small, sent as is) | 194 ns | 0 | 0 |
| `Compress`, 3.6 KB text, client without gzip | 170 ns | 0 | 0 |
| `Compress`, 3.6 KB text, gzip `BestSpeed` | 2.25 µs | 0 | 0 |

- `FromStd` allocates what a net/http handler is allowed to keep: the `*http.Request`, its URL, the header map
  (all values share one string), and the request context. The ResponseWriter wrapper is pooled.
- `FromStdMiddleware` pays for `FromStd` plus `ToStd`, so compose net/http middleware into one chain and adapt
  it once.
- The compression middleware and its gzip writers are pooled: no allocations per request.

## M3: ServeMux dispatch

- `BenchmarkServeMux` in the root package (not this module): the same 10 patterns are registered in
  `qilat.ServeMux` and `net/http.ServeMux`, and each mux dispatches a prepared request to a no-op writer, so
  only routing is measured. Raw output in [`results/m3-servemux.txt`](results/m3-servemux.txt).
- Same machine and Go version as M2, `-count 10`, medians.

```sh
go test -run '^$' -bench 'BenchmarkServeMux$' -benchmem -count 10 .
```

| Request | Matching pattern | net/http | qilat | net/http allocs | qilat allocs |
|---|---|---|---|---|---|
| `/static/app.js` | `GET /static/app.js` | 59.1 ns | **48.7 ns** | 0 | 0 |
| `/users/42` | `GET /users/{id}` | 57.5 ns | **49.1 ns** | 1 | 0 |
| `/users/42/posts/7` | `GET /users/{id}/posts/{post}` | 96.3 ns | **80.7 ns** | 2 | 0 |
| `/files/a/b/c` | `GET /files/{path...}` | 148 ns | **92.1 ns** | 5 | 0 |
| `/nope` | none (404) | 350 ns | **253 ns** | 16 | 8 |

- Matching never allocates in qilat: wildcard values are decoded into buffers owned by the pooled request.
  Calling `r.PathValue` copies the value (1 allocation); `r.PeekPathValue` does not.
- A miss allocates in both muxes to compute the 405 `Allow` list and build the error handler.

## M2: qilat HTTP/1.1 server

- Same machine and flags as the M0 baseline: Apple M5 (10 cores), macOS 26.4, Go 1.27.3, fasthttp v1.77.0,
  `-benchtime 1s -count 5 -parallelism 8`. Raw output in [`results/m2.txt`](results/m2.txt).
- Medians over 5 runs; differences below ~5% are noise.

### Server cost: `inmem` (no kernel), time per request, lower is better

| Scenario | net/http | fasthttp | qilat | qilat allocs/op |
|---|---|---|---|---|
| plaintext | 1.12 µs | 250 ns | **245 ns** | 0 |
| json-1kb | 1.04 µs | 259 ns | **241 ns** | 0 |
| headers-20 | 1.80 µs | **350 ns** | 431 ns | 0 |
| upload-64kb | 1.11 µs | 844 ns | **766 ns** | 1 (handler) |

### Pipelined, 16 requests per write

| Scenario | net/http | fasthttp | qilat |
|---|---|---|---|
| plaintext | 2.16 µs | 372 ns | **355 ns** |
| json-1kb | 2.24 µs | 782 ns | **716 ns** |
| headers-20 | 2.92 µs | 685 ns | **546 ns** |

### TCP keep-alive and new connection per request

On this machine these modes are kernel-bound (see the M0 observations), and all three servers are within ~10%:
keep-alive plaintext 5.97 µs (net/http), 6.42 µs (fasthttp), 5.85 µs (qilat); new connection 33.6, 36.3, 34.6 µs.

### Allocations

- qilat: **0 allocs/op** for every keep-alive, pipelined, and in-memory scenario except the upload, whose single
  allocation is the benchmark handler formatting the length (fasthttp's handler has the same one).
- New connections cost 25 allocs (connection setup, goroutine, buffers): fasthttp 24, net/http 56.

### Observations

- qilat meets the M2 target: faster than net/http in every server-bound scenario (4–5× in `inmem`) and on par with
  fasthttp, ahead in most scenarios.
- The remaining gap is `headers-20` in `inmem` (~23% behind fasthttp): each header is validated, lowercased, and
  copied into the request. Pipelined, where syscalls are amortized but not removed, qilat is ahead.
- The HTTP/2 rows (net/http only) are unchanged; qilat HTTP/2 arrives in M4.

## M0 baseline

- Machine: Apple M5 (10 cores), 16 GB RAM, macOS 26.4
- Go 1.27.3, fasthttp v1.77.0
- `-benchtime 1s -count 5 -parallelism 8`, raw output in [`results/m0-baseline.txt`](results/m0-baseline.txt)
- Values are medians over 5 runs. 5 samples are too few for a benchstat confidence interval, so treat
  differences below ~5% as noise; use `-count 10` for comparisons.

### HTTP/1.1, time per request (lower is better)

| Scenario | Mode | net/http | fasthttp |
|---|---|---|---|
| plaintext | keepalive | 5.78 µs | 6.37 µs |
| plaintext | newconn | 32.5 µs | 35.3 µs |
| plaintext | pipeline16 | 2.02 µs | 0.375 µs |
| plaintext | inmem | 0.970 µs | 0.235 µs |
| json-1kb | keepalive | 5.80 µs | 6.42 µs |
| json-1kb | pipeline16 | 2.02 µs | 0.773 µs |
| json-1kb | inmem | 1.03 µs | 0.257 µs |
| headers-20 | keepalive | 6.32 µs | 6.35 µs |
| headers-20 | pipeline16 | 2.95 µs | 0.683 µs |
| headers-20 | inmem | 1.91 µs | 0.356 µs |
| upload-64kb | keepalive | 9.92 µs | 9.26 µs |
| upload-64kb | inmem | 1.15 µs | 0.925 µs |

### HTTP/1.1, allocations per request

| Scenario | Mode | net/http | fasthttp |
|---|---|---|---|
| plaintext | keepalive / inmem | 20 / 22 allocs, ~2.2 KB | 0 allocs, ~0 B |
| headers-20 | keepalive / inmem | 62 / 64 allocs, ~4.6 KB | 0 allocs, ~0 B |
| upload-64kb | keepalive / inmem | 25 / 27 allocs, ~2.4 KB | 1 alloc, ≤71 B |
| any | newconn | 56–98 allocs, 4.3–6.8 KB | 24–25 allocs, ~1.2–1.4 KB |

### HTTP/2 cleartext (net/http server and client)

| Scenario | Time per request | allocs/op | B/op |
|---|---|---|---|
| plaintext | 12.3 µs | 86 | 8.9 KB |
| json-1kb | 15.5 µs | 89 | 11.9 KB |
| headers-20 | 21.5 µs | 169 | 19.0 KB |
| upload-64kb | 51.9 µs | 131 | 10.5 KB |

### Observations

- **TCP keep-alive and newconn are kernel-bound on this machine.** Both servers land within ~10% of each other
  regardless of connection count (tested with `-parallelism` 1, 8, 32), so these modes cannot tell server
  implementations apart on macOS. Use `inmem` and `pipeline16` to compare server cost, and keep the TCP modes
  as a sanity check for real-socket behavior.
- In `inmem`, fasthttp is ~4–5× faster than net/http on small requests (~1.2× on the 64 KB upload, where
  copying the body dominates) with zero allocations. That is the bar qilat's
  HTTP/1.1 server (M2) is measured against.
- net/http allocation count grows with request header count (20 → 62 allocs); fasthttp's does not.
  qilat's `Header` design (M1) targets the fasthttp behavior.

### Not measured yet

- External load generators (`h2load`, `oha`) are not installed on the baseline machine, so there are no
  out-of-process numbers yet. They will be added together with TLS-based HTTP/2 and HTTP/3 benchmarks in M4/M5.
- HTTP/3 has no baseline: neither net/http nor fasthttp supports it. `quic-go/http3` becomes the comparison point in M5.
