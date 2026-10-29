package qilat

// Handler responds to an HTTP request. Like [net/http.Handler], it must not use w or r after ServeHTTP returns;
// in qilat both are reused for later requests.
type Handler interface {
	ServeHTTP(w ResponseWriter, r *Request)
}

// HandlerFunc adapts an ordinary function to a [Handler].
type HandlerFunc func(w ResponseWriter, r *Request)

// ServeHTTP calls f(w, r).
func (f HandlerFunc) ServeHTTP(w ResponseWriter, r *Request) { f(w, r) }
