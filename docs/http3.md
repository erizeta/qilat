# Deploying HTTP/3

HTTP/3 runs over QUIC, which runs over UDP. qilat serves it from the same `Server` and handlers as HTTP/1.1
and HTTP/2, but the network around the server has to let UDP through. This page covers what to check.

## How clients find HTTP/3

Browsers and curl do not try HTTP/3 first. They connect over TCP, and switch when a response carries an
`Alt-Svc` header (RFC 7838) saying where HTTP/3 is available:

```
alt-svc: h3=":443"; ma=86400
```

`ListenAndServeTLS` and `ServeQUIC` add this header to every HTTP/1.1 and HTTP/2 response while the QUIC
listener runs. The port is the local UDP port, and `ma` (how long clients remember it, in seconds) comes from
`Server.HTTP3.AltSvcMaxAge`, 24 hours by default. HTTP/3 responses do not carry it.

- If clients reach the server through a different port, for example a load balancer that forwards UDP 443 to
  8443, the advertised port is wrong. Set `HTTP3.DisableAltSvc` and set the header yourself, in a handler or
  middleware: a handler's own `Alt-Svc` always wins over the automatic one.
- Clients that tried HTTP/3 and failed fall back to TCP, so a blocked UDP port costs a delay on the first
  request, not an outage. Browsers then avoid HTTP/3 for that host for a while.
- To stop serving HTTP/3, first set `DisableAltSvc` (or a short `AltSvcMaxAge`) and wait for the old `ma` to
  pass, so clients stop trying it.

## Firewalls, load balancers, and proxies

- Open **UDP** on the HTTPS port (usually 443) in host firewalls, security groups, and network ACLs. TCP rules
  do not cover it.
- A load balancer in front of qilat must forward UDP and keep packets of one QUIC connection on the same
  backend. Layer-4 (UDP) load balancing with connection affinity works; HTTP-level load balancers usually
  terminate HTTP/3 themselves and talk to the backend over HTTP/1.1 or HTTP/2, in which case qilat does not
  need to serve HTTP/3 at all.
- QUIC connections can migrate when a client's address changes (a phone moving from Wi-Fi to mobile data).
  Balancers that route only by source address may send the migrated connection to another backend, which
  then drops it, and the client reconnects.
- Some corporate networks block UDP 443 entirely. Clients fall back to TCP, so keep HTTP/1.1 and HTTP/2
  serving on TCP.

To serve HTTP/3 on a socket you set up yourself, such as one from systemd socket activation or one bound to a
specific address, pass it to `ServeQUIC` and serve TCP separately:

```go
pc, err := net.ListenPacket("udp", ":443")
if err != nil {
	log.Fatal(err)
}
go func() { log.Fatal(srv.ServeQUIC(pc)) }()
log.Fatal(srv.ListenAndServeTLS(":443", "cert.pem", "key.pem")) // with DisableHTTP3, or a different port
```

`ListenAndServeTLS` opens both listeners on the same port and, if either fails, closes the other and returns
the error.

## TLS

QUIC always uses TLS 1.3, whatever `TLSConfig.MinVersion` says; TCP connections keep using your settings. The
same certificates serve both. `TLSConfig` is cloned, never modified. qilat sets the ALPN protocol to `h3` for
QUIC, and for TCP adds `h2` to your `NextProtos` (`h2` and `http/1.1` when you set none).

`qilat.GenerateDevCert` makes a self-signed certificate for local testing. Browsers reject self-signed
certificates for HTTP/3 even when you accept them for HTTPS, so test HTTP/3 in a browser with a certificate
from a local CA such as [mkcert](https://github.com/FiloSottile/mkcert), or with `curl -k --http3-only`.

## UDP buffer sizes on Linux

QUIC receives many small packets, and Linux's default socket buffers are small enough to drop packets under
load. quic-go tries to raise the buffers and logs a message such as
`failed to sufficiently increase receive buffer size` when the kernel limit prevents it. Raise the limits:

```sh
sudo sysctl -w net.core.rmem_max=7500000
sudo sysctl -w net.core.wmem_max=7500000
```

Add the same keys to `/etc/sysctl.d/` to keep them after a reboot. In containers, these are host settings:
set them on the node. On BSD and macOS the matching limit is `kern.ipc.maxsockbuf`. See the
[quic-go documentation](https://github.com/quic-go/quic-go/wiki/UDP-Buffer-Sizes) for details.

## 0-RTT

With `Server.HTTP3.Allow0RTT`, a client that connected before can send its first request together with the
handshake, saving a round trip. The cost is that 0-RTT data can be **replayed**: an attacker who captured the
packets can send them again, and the server cannot tell the copy from the original until the handshake ends.

0-RTT is off by default. When it is on, qilat answers `425 Too Early` (RFC 8470) to requests that arrive
before the handshake completes unless their method is idempotent: `GET`, `HEAD`, `OPTIONS`, `TRACE`, `PUT`, or
`DELETE`. The client then retries after the handshake. Enable it only if every handler reached by those
methods is safe to run twice, which includes GET handlers with side effects and PUT or DELETE handlers whose
replay at a later time would undo a change made since.

## Limits

`Server.HTTP3` tunes the HTTP/3 limits; zero fields are safe defaults.

| Field | Default | What it bounds |
|---|---|---|
| `MaxConcurrentStreams` | 250 | Concurrent requests, and running handlers, per connection |
| `MaxReceiveBufferPerStream` | 1 MiB | Request body data a client may send ahead of the handler reading it |
| `MaxReceiveBufferPerConnection` | 4 MiB | The same for all streams of a connection |

`Server.MaxHeaderBytes` limits the request header section, `MaxRequestBodySize` the body, `IdleTimeout` the
QUIC idle timeout, and `ReadHeaderTimeout` the QUIC handshake. QPACK uses only the static table, so a client
cannot make the server keep a dynamic table, and request headers are decoded without allocating.

## Checking that it works

```sh
curl -sI https://example.com/ | grep -i alt-svc       # the TCP response advertises HTTP/3
curl --http3-only -sI https://example.com/            # needs a curl built with HTTP/3 (curl -V lists HTTP3)
```

In Chrome, open DevTools, Network, and add the Protocol column: requests after the first show `h3`.

On a development machine with a certificate from a local CA such as mkcert, Chrome may keep using `h2` even
though it received the `Alt-Svc` header. To check the server itself, start Chrome with HTTP/3 forced for the
origin, in a separate profile:

```sh
open -na "Google Chrome" --args --user-data-dir=/tmp/chrome-h3 --origin-to-force-quic-on=localhost:8443 https://localhost:8443
```

curl can check the `Alt-Svc` switch without a browser: with a cache file, the first request uses HTTP/2 and
stores the advertisement, and the second uses HTTP/3.

```sh
curl -sk --alt-svc /tmp/altsvc.txt -o /dev/null -w '%{http_version}\n' https://localhost:8443   # 2
curl -sk --alt-svc /tmp/altsvc.txt -o /dev/null -w '%{http_version}\n' https://localhost:8443   # 3
```

## Interoperability

Tested on 2026-10-30 with qilat at v0.1.0 (quic-go v0.64.1) on macOS, serving the README's `ListenAndServeTLS`
quick start on port 8443 with a certificate from mkcert.

| Client | Result |
|---|---|
| curl 8.23.0 (ngtcp2 1.25.0, nghttp3 1.18.0), `--http1.1`, `--http2`, `--http3-only` | ✅ Each protocol served |
| curl 8.23.0 with `--alt-svc` cache | ✅ Second request switches to HTTP/3; cached for the advertised `ma` (24 hours) |
| Chrome 156.0.8078.74 with `--origin-to-force-quic-on` | ✅ `h3` |
| Chrome 156.0.8078.74, discovery through `Alt-Svc` only | ⚠️ Stayed on `h2` with the local-CA certificate; see above |
| quic-go v0.64.1 `http3.Transport` against the qilat server (automated: `http3_test.go`, `examples/std-middleware`, benchmarks) | ✅ |
| qilat `Client` against quic-go's `http3.Server`, found through `Alt-Svc` (automated: `TestClientH3AgainstReference`) | ✅ |
