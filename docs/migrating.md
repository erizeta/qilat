# Migrating to qilat

qilat keeps net/http's shape (a `Handler` with `ServeHTTP(w, r)`, a `ServeMux` with Go 1.22 patterns, a
`Server` with `Shutdown`) and fasthttp's performance model (pooled requests, `[]byte` accessors, zero
allocations on the hot path). Most code moves over mechanically; the one rule to learn is the
[request lifetime](lifetime.md).

## From net/http

### Step 1: run your existing handler on qilat

`qilat.FromStd` adapts any `http.Handler`, so a whole application can move to qilat's server before any handler
changes, and gain HTTP/3:

```go
mux := http.NewServeMux() // your existing net/http routes
// ...
srv := &qilat.Server{Handler: qilat.FromStd(mux)}
log.Fatal(srv.ListenAndServeTLS(":443", "cert.pem", "key.pem"))
```

`FromStd` gives the handler a real `*http.Request` and an `http.ResponseWriter` with net/http semantics. It
costs about 10 allocations per request, which native handlers avoid; see
[benchmarks](../benchmarks/RESULTS.md#m6-nethttp-adapters-and-compression).

### Step 2: convert hot routes to native handlers

Convert the routes that matter most and leave the rest on `FromStd`. A qilat `ServeMux` can mount the old
mux for everything it does not handle itself:

```go
mux := qilat.NewServeMux()
mux.HandleFunc("GET /api/items/{id}", getItem) // native
mux.Handle("/", qilat.FromStd(legacyMux))      // everything else, unchanged
```

net/http middleware works through `qilat.FromStdMiddleware`; compose it into one chain and adapt it once.

### API mapping

| net/http | qilat |
|---|---|
| `func(w http.ResponseWriter, r *http.Request)` | `func(w qilat.ResponseWriter, r *qilat.Request)` |
| `r.Method` | `r.Method()` |
| `r.URL.Path` | `r.URI().Path()` (decoded, `[]byte`) or `string(r.Path())` |
| `r.URL.RawQuery`, `r.URL.Query().Get(k)` | `r.URI().RawQuery()`, `r.URI().Query().Get(k)` |
| `r.Header.Get(k)` | `r.Header.Get(k)`, or `r.Header.Peek(k)` without allocating |
| `r.Header.Values(k)` | `r.Header.PeekAll(k)` |
| `r.Host`, `r.RemoteAddr`, `r.TLS` | `r.Host()` (`[]byte`), `r.RemoteAddr()` (`net.Addr`), `r.TLS()` |
| `r.Body` (`io.ReadCloser`) | `r.Body` (`io.Reader`, never nil; no need to close) |
| `r.ContentLength`, `r.Proto`, `r.Pattern` | `r.ContentLength()`, `r.Proto()`, `r.Pattern()` |
| `r.PathValue(k)`, `r.Context()`, `r.WithContext`, `r.Clone` | the same |
| `w.Header()` (`http.Header` map) | `w.Header()` (`*qilat.Header`: `Set`, `Add`, `Get`, `Del`) |
| `w.(http.Flusher).Flush()` | `w.Flush()` |
| `w.(http.Hijacker).Hijack()` | `w.(qilat.Hijacker).Hijack()` (HTTP/1.x only) |
| `w.Header().Set("Trailer", ...)` then set after writing | `w.(qilat.TrailerSetter).Trailer().Set(k, v)` |
| `http.Error`, `http.NotFound`, `http.StripPrefix` | `qilat.Error`, `qilat.NotFound`, `qilat.StripPrefix` |
| `http.FileServerFS`, `http.ServeFile`, `http.ServeContent` | `qilat.FileServer`, `qilat.ServeFile`, `qilat.ServeContent` |
| `http.NewServeMux`, `mux.Handle`, `mux.HandleFunc` | `qilat.NewServeMux`, the same methods and pattern syntax |
| `http.ErrAbortHandler`, `http.ErrServerClosed` | `qilat.ErrAbortHandler`, `qilat.ErrServerClosed` |

Not provided natively yet: cookie helpers (`r.Cookie`, `http.SetCookie`), `r.ParseForm` and `r.FormValue`,
and multipart forms. Read the `Cookie` header with `r.Header.Peek("cookie")` and set `Set-Cookie` with
`w.Header().Add`; decode a URL-encoded form with `qilat.Args`:

```go
body, err := io.ReadAll(r.Body)
if err != nil {
	qilat.Error(w, "bad request", http.StatusBadRequest)
	return
}
var form qilat.Args
form.Parse(body)
name := form.Get("name")
```

Or keep those routes on `FromStd`, where everything in net/http works.

### Server configuration

| `http.Server` | `qilat.Server` |
|---|---|
| `Addr` | the argument of `ListenAndServe(addr)` |
| `Handler`, `TLSConfig`, `BaseContext` | the same |
| `ReadHeaderTimeout`, `IdleTimeout`, `MaxHeaderBytes` | the same, but **zero means a safe default** (10 s, 120 s, 64 KiB), not unlimited |
| `ReadTimeout`, `WriteTimeout` | the same; zero means no limit, as in net/http |
| none | `MaxRequestBodySize` (4 MiB by default), `MaxConcurrentConns` |
| `Protocols` | `DisableHTTP2`, `H2C`, `DisableHTTP3` |
| `HTTP2` | `HTTP2` (`qilat.HTTP2Config`) |
| `ErrorLog` (`*log.Logger`) | `ErrorLog` (`*slog.Logger`) |
| `ListenAndServeTLS(cert, key)` | `ListenAndServeTLS(addr, cert, key)`, which also serves HTTP/3 over UDP |
| `Shutdown`, `Close`, `Serve`, `ServeTLS` | the same |

Set a limit to a negative value to disable it. The 4 MiB body limit is the most common surprise when
migrating an upload endpoint: raise `MaxRequestBodySize`, or set it to `-1`.

### Things that behave differently

- **Lifetime.** Requests, headers, and the `ResponseWriter` are reused after the handler returns. Copy what
  you keep; see [lifetime.md](lifetime.md).
- **Header names are lowercase.** `r.Header.All()` yields lowercase names, and lookups are case-insensitive.
  Adding a header with an invalid name panics; invalid characters in values are replaced, which prevents
  header injection.
- **Encoded slashes are never decoded for routing.** `%2F` in a path stays a literal character of a segment,
  even where net/http would decode it.
- **`StripPrefix`** always leaves a path that starts with `/`.

### Client

| net/http | qilat |
|---|---|
| `resp, err := client.Get(url)` | `resp := qilat.AcquireResponse()`, `err := client.Get(url, resp)` |
| `http.NewRequestWithContext(ctx, m, url, body)` | `req := qilat.AcquireRequest()`, `req.SetMethod(m)`, `req.SetRequestURI(url)`, `req.SetBody(body, n)` |
| `client.Do(req)` | `client.Do(req, resp)`, or `client.Do(req.WithContext(ctx), resp)` to cancel it; release `req` itself, not the copy |
| `resp.StatusCode`, `resp.Header`, `resp.Trailer` | `resp.StatusCode()`, `resp.Header`, `resp.Trailer()` |
| `io.ReadAll(resp.Body)`, `resp.Body.Close()` | `resp.ReadBody()`, `resp.Close()` or `qilat.ReleaseResponse(resp)` |
| `http.Transport{...}` | `qilat.Transport{...}`, with `EnableHTTP3` for HTTP/3 via Alt-Svc |
| `http.ProxyFromEnvironment` | `qilat.ProxyFromEnvironment` |

Release requests and responses when you are done with them. A response's body slice belongs to the response.

## From fasthttp

The lifetime rules are the same as fasthttp's, so code that already respects them carries over. The main
changes are the handler shape and that request and response are two values instead of one `RequestCtx`.

| fasthttp | qilat |
|---|---|
| `func(ctx *fasthttp.RequestCtx)` | `func(w qilat.ResponseWriter, r *qilat.Request)` |
| `ctx.Method()` (`[]byte`) | `r.Method()` (`string`, no allocation for standard methods) |
| `ctx.Path()`, `ctx.Host()` | `r.Path()`, `r.Host()` |
| `ctx.QueryArgs().Peek(k)` | `r.URI().Query().Peek(k)` |
| `ctx.Request.Header.Peek(k)` | `r.Header.Peek(k)` |
| `ctx.PostBody()` | `io.ReadAll(r.Body)`; the body streams, within `MaxRequestBodySize` |
| `ctx.PostArgs()` | read the body and parse it with `qilat.Args` |
| `ctx.UserValue(k)` (with a router) | `r.PathValue(k)` or `r.PeekPathValue(k)` with `qilat.ServeMux` |
| `ctx.SetStatusCode(c)` | `w.WriteHeader(c)` |
| `ctx.SetContentType(t)`, `ctx.Response.Header.Set(k, v)` | `w.Header().Set("Content-Type", t)`, `w.Header().Set(k, v)` |
| `ctx.SetBody(b)`, `ctx.WriteString(s)` | `w.Write(b)`, `w.WriteString(s)` |
| `ctx.SetBodyStreamWriter(f)` | write in the handler and call `w.Flush()` as you go |
| `ctx.Done()` (closed only on shutdown) | `r.Context().Done()`, closed when the client disconnects or the handler returns |
| `ctx.RemoteAddr()`, `ctx.IsTLS()` | `r.RemoteAddr()`, `r.TLS() != nil` |
| `ctx.Hijack(f)` | `w.(qilat.Hijacker).Hijack()` |
| `fasthttp.Server{Handler: h}`, `ListenAndServe(addr)` | `qilat.Server{Handler: h}`, `ListenAndServe(addr)` |
| `fasthttp.AcquireRequest`, `AcquireResponse`, `client.Do(req, resp)` | `qilat.AcquireRequest`, `AcquireResponse`, `client.Do(req, resp)` |
| `resp.Body()` | `resp.ReadBody()` |
| `client.DoTimeout(req, resp, d)` | `Client.Timeout`, or a context with a deadline on the request |

What you gain: HTTP/2 and HTTP/3 from the same `Server`, per-request cancellation, `Shutdown` that drains
requests, net/http middleware through `FromStdMiddleware`, and handlers that can be mounted on a net/http server
with `qilat.ToStd`.
