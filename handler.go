package qilat

import "errors"

// ErrAbortHandler is a sentinel panic value to abort a handler. The connection or stream is closed
// and, unlike other panics, nothing is logged.
var ErrAbortHandler = errors.New("qilat: abort Handler")

// Handler responds to an HTTP request. Like [net/http.Handler], it must not use w or r after ServeHTTP returns;
// in qilat both are reused for later requests.
type Handler interface {
	ServeHTTP(w ResponseWriter, r *Request)
}

// HandlerFunc adapts an ordinary function to a [Handler].
type HandlerFunc func(w ResponseWriter, r *Request)

// ServeHTTP calls f(w, r).
func (f HandlerFunc) ServeHTTP(w ResponseWriter, r *Request) { f(w, r) }

// Error replies with the plain-text message msg and status code, like [net/http.Error].
func Error(w ResponseWriter, msg string, code int) {
	h := w.Header()
	h.Del("content-length")
	h.Set("content-type", "text/plain; charset=utf-8")
	h.Set("x-content-type-options", "nosniff")
	w.WriteHeader(code)
	_, _ = w.WriteString(msg)
	_, _ = w.WriteString("\n")
}

// NotFound replies with a 404 Not Found error.
func NotFound(w ResponseWriter, _ *Request) { Error(w, "404 page not found", 404) }
