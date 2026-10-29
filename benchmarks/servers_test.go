package benchmarks

import (
	"context"
	"errors"
	"io"
	"log"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/erizeta/qilat"
	"github.com/valyala/fasthttp"
)

// serverImpl names a server under test; start serves on ln until the returned stop function is called.
type serverImpl struct {
	name  string
	start func(tb testing.TB, ln net.Listener) (stop func())
}

var h1Servers = []serverImpl{
	{name: "nethttp", start: startNetHTTP(false)},
	{name: "fasthttp", start: startFastHTTP},
	{name: "qilat", start: startQilat},
}

var h2Servers = []serverImpl{
	{name: "nethttp", start: startNetHTTP(true)},
}

func listen(tb testing.TB) net.Listener {
	tb.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		tb.Fatalf("listen: %v", err)
	}
	return ln
}

func netHTTPHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /plaintext", func(w http.ResponseWriter, _ *http.Request) {
		writeNetHTTP(w, "text/plain; charset=utf-8", []byte(plaintextBody))
	})
	mux.HandleFunc("GET /json", func(w http.ResponseWriter, _ *http.Request) {
		writeNetHTTP(w, "application/json", jsonBody)
	})
	mux.HandleFunc("GET /headers", func(w http.ResponseWriter, r *http.Request) {
		writeNetHTTP(w, "text/plain; charset=utf-8", []byte(r.Header.Get(targetHeader)))
	})
	mux.HandleFunc("POST /upload", func(w http.ResponseWriter, r *http.Request) {
		n, err := io.Copy(io.Discard, r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		writeNetHTTP(w, "text/plain; charset=utf-8", strconv.AppendInt(nil, n, 10))
	})
	return mux
}

// Explicit Content-Length keeps net/http from switching to chunked encoding, matching fasthttp.
func writeNetHTTP(w http.ResponseWriter, contentType string, body []byte) {
	h := w.Header()
	h.Set("Content-Type", contentType)
	h.Set("Content-Length", strconv.Itoa(len(body)))
	_, _ = w.Write(body)
}

func startNetHTTP(h2c bool) func(testing.TB, net.Listener) func() {
	return func(tb testing.TB, ln net.Listener) func() {
		srv := &http.Server{
			Handler:           netHTTPHandler(),
			ReadHeaderTimeout: 10 * time.Second,
			ErrorLog:          log.New(io.Discard, "", 0),
		}
		if h2c {
			var p http.Protocols
			p.SetHTTP1(true)
			p.SetUnencryptedHTTP2(true)
			srv.Protocols = &p
		}
		done := make(chan error, 1)
		go func() { done <- srv.Serve(ln) }()
		return func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := srv.Shutdown(ctx); err != nil {
				tb.Errorf("nethttp shutdown: %v", err)
			}
			if err := <-done; !errors.Is(err, http.ErrServerClosed) {
				tb.Errorf("nethttp serve: %v", err)
			}
		}
	}
}

func fastHTTPHandler(ctx *fasthttp.RequestCtx) {
	switch string(ctx.Path()) {
	case "/plaintext":
		ctx.SetContentType("text/plain; charset=utf-8")
		ctx.SetBodyString(plaintextBody)
	case "/json":
		ctx.SetContentType("application/json")
		ctx.SetBody(jsonBody)
	case "/headers":
		ctx.SetContentType("text/plain; charset=utf-8")
		ctx.SetBody(ctx.Request.Header.Peek(targetHeader))
	case "/upload":
		ctx.SetContentType("text/plain; charset=utf-8")
		ctx.SetBody(strconv.AppendInt(nil, int64(len(ctx.PostBody())), 10))
	default:
		ctx.SetStatusCode(fasthttp.StatusNotFound)
	}
}

type discardLogger struct{}

func (discardLogger) Printf(string, ...any) {}

func startFastHTTP(tb testing.TB, ln net.Listener) func() {
	srv := &fasthttp.Server{
		Handler:     fastHTTPHandler,
		ReadTimeout: 10 * time.Second,
		Logger:      discardLogger{},
	}
	done := make(chan error, 1)
	go func() { done <- srv.Serve(ln) }()
	return func() {
		if err := srv.ShutdownWithContext(context.Background()); err != nil {
			tb.Errorf("fasthttp shutdown: %v", err)
		}
		if err := <-done; err != nil {
			tb.Errorf("fasthttp serve: %v", err)
		}
	}
}

func qilatHandler(w qilat.ResponseWriter, r *qilat.Request) {
	switch string(r.Path()) {
	case "/plaintext":
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = w.WriteString(plaintextBody)
	case "/json":
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(jsonBody)
	case "/headers":
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = w.Write(r.Header.Peek(targetHeader))
	case "/upload":
		n, err := io.Copy(io.Discard, r.Body)
		if err != nil {
			w.WriteHeader(400)
			return
		}
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		var num [20]byte
		_, _ = w.Write(strconv.AppendInt(num[:0], n, 10))
	default:
		w.WriteHeader(404)
	}
}

func startQilat(tb testing.TB, ln net.Listener) func() {
	srv := &qilat.Server{
		Handler:  qilat.HandlerFunc(qilatHandler),
		ErrorLog: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	done := make(chan error, 1)
	go func() { done <- srv.Serve(ln) }()
	return func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := srv.Shutdown(ctx); err != nil {
			tb.Errorf("qilat shutdown: %v", err)
		}
		if err := <-done; !errors.Is(err, qilat.ErrServerClosed) {
			tb.Errorf("qilat serve: %v", err)
		}
	}
}
