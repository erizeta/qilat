# Benchmark results

Comparison benchmarks for qilat against net/http and fasthttp. qilat itself is added from M2 onward;
this file currently holds the **M0 baseline** that later milestones are measured against.

## How to run

```sh
cd benchmarks
go test -run '^$' -bench . -benchmem -benchtime 1s -count 10 ./... > new.txt
go run golang.org/x/perf/cmd/benchstat@latest results/m0-baseline.txt new.txt
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
