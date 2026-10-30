# Request and response lifetime

qilat reuses its request and response objects, as fasthttp does. That reuse is what keeps a request free of
allocations, and it comes with one rule this page explains.

## The rule

On a server, a `*Request`, its `Header`, its `Body`, its `URI` and `Args`, the `ResponseWriter`, and every
`[]byte` any of them return are valid **only until `ServeHTTP` returns**. After that, qilat hands the same
objects and buffers to the next request, possibly from another client.

On a client, a `*Request` is valid until `ReleaseRequest`, and a `*Response`, its `Header`, `Body`, `Trailer`,
and the slice from `ReadBody` until `ReleaseResponse`.

## What is safe to keep

| Value | Safe to keep after the handler returns? |
|---|---|
| `r.Context()` | **Yes.** It is never reused, and is canceled when the client disconnects or the handler returns. |
| `r.Method()`, `r.Proto()`, `r.Pattern()` | Yes: the strings they return are never reused. |
| `r.Header.Get(name)`, `r.URI().Query().Get(key)`, `r.PathValue(name)` | Yes: they return a new `string`. |
| `r.Header.Peek(name)`, `r.Path()`, `r.Host()`, `r.PeekPathValue(name)`, `Args.Peek` | **No**: they return bytes owned by the request. |
| `r`, `r.Header`, `r.Body`, `r.URI()`, `w` | **No.** |
| `r.Clone(ctx)` | Yes: a deep copy, with an empty body. |

The `Get` methods allocate a string; the `Peek` methods do not. Use `Peek` on hot paths where the bytes are
used before the handler returns, and `Get` or a copy when they are not.

## Correct and incorrect use

Passing request data to a goroutine:

```go
// Wrong: path is a slice of the request's buffer, which the next request overwrites.
func handler(w qilat.ResponseWriter, r *qilat.Request) {
	path := r.Path()
	go audit(path)
}

// Right: copy what the goroutine needs.
func handler(w qilat.ResponseWriter, r *qilat.Request) {
	path := string(r.Path())
	go audit(path)
}
```

Keeping the whole request, for example to process it in the background:

```go
func handler(w qilat.ResponseWriter, r *qilat.Request) {
	body, err := io.ReadAll(r.Body) // the clone has no body, so read it first
	if err != nil {
		qilat.Error(w, "bad body", http.StatusBadRequest)
		return
	}
	req := r.Clone(context.WithoutCancel(r.Context()))
	go process(req, body)
	w.WriteHeader(http.StatusAccepted)
}
```

`context.WithoutCancel` keeps the request's values but not its cancellation, which happens when the handler
returns. Pass `r.Context()` itself to work that must stop when the client goes away.

Working with the response after returning:

```go
// Wrong: the ResponseWriter is reused once the handler returns.
func handler(w qilat.ResponseWriter, r *qilat.Request) {
	go func() {
		time.Sleep(time.Second)
		_, _ = w.WriteString("late") // writes into another request's response, or panics in debug mode
	}()
}
```

A handler that streams must stay in `ServeHTTP` until it is done, and can use `r.Context()` to stop early:

```go
func handler(w qilat.ResponseWriter, r *qilat.Request) {
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case now := <-t.C:
			_, _ = w.WriteString(now.Format(time.RFC3339) + "\n")
			_ = w.Flush()
		}
	}
}
```

## Client requests and responses

```go
req := qilat.AcquireRequest()
defer qilat.ReleaseRequest(req)
resp := qilat.AcquireResponse()
defer qilat.ReleaseResponse(resp)

if err := req.SetRequestURI("https://example.com/"); err != nil {
	return err
}
if err := client.Do(req.WithContext(ctx), resp); err != nil { // the copy shares req's buffers; release req
	return err
}
body, err := resp.ReadBody() // owned by resp: copy it to keep it past ReleaseResponse
```

`ReadBody` reuses one buffer per `Response`, so a second `ReadBody` or `Do` with the same `resp` overwrites
the slice the first returned. Read the body to EOF, or release the response, so the connection can be reused.

## Debug mode

Set `Server.DebugPoison` while developing or in tests:

```go
srv := &qilat.Server{Handler: h, DebugPoison: true}
```

Requests and ResponseWriters are then not returned to the pool. Instead they are marked dead when the handler
returns, and later use of the request, its `Header`, or the `ResponseWriter` panics with a message such as
`qilat: Header used after its request was released`. Because nothing is reused, slices kept by mistake stay
intact in this mode, so run your tests with `-race` as well: it reports goroutines that still read request
data while the next request writes it.

`DebugPoison` costs allocations on every request, so leave it off in production.

## net/http handlers

Handlers wrapped with `qilat.FromStd` follow net/http rules instead: the `*http.Request` they receive, its URL,
and its header map are new objects and may be kept. Its `Body` may not be read after the handler returns.
