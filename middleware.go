package qilat

import (
	"bytes"
	"net/http"
	"slices"
)

// Chain composes middleware so that Chain(a, b)(h) is a(b(h)): a sees the request first.
func Chain(mw ...func(Handler) Handler) func(Handler) Handler {
	mw = slices.Clone(mw)
	return func(h Handler) Handler {
		for i := len(mw) - 1; i >= 0; i-- {
			h = mw[i](h)
		}
		return h
	}
}

// FromStdMiddleware adapts net/http middleware, such as CORS or request logging, to qilat handlers.
// Each request pays for both [FromStd] and [ToStd].
func FromStdMiddleware(mw func(http.Handler) http.Handler) func(Handler) Handler {
	return func(next Handler) Handler {
		return FromStd(mw(ToStd(next)))
	}
}

// StripPrefix serves requests whose path starts with prefix by removing it and calling h, like
// [net/http.StripPrefix]; other requests get 404. The remaining path always starts with a slash.
func StripPrefix(prefix string, h Handler) Handler {
	if prefix == "" {
		return h
	}
	p := []byte(prefix)
	return HandlerFunc(func(w ResponseWriter, r *Request) {
		// The raw path must match too, or an escaped prefix such as /st%61tic/ would be stripped.
		raw := r.URI().RawPath()
		if !bytes.HasPrefix(r.Path(), p) || !bytes.HasPrefix(raw, p) {
			NotFound(w, r)
			return
		}
		rest := raw[len(p):]
		target := make([]byte, 0, len(rest)+len(r.uri.rawQuery)+2)
		if len(rest) == 0 || rest[0] != '/' {
			target = append(target, '/')
		}
		target = append(target, rest...)
		if r.uri.rawQuery != nil {
			target = append(append(target, '?'), r.uri.rawQuery...)
		}
		r2 := new(Request)
		*r2 = *r
		r2.uri = URI{}
		if r2.uri.Parse(target) != nil {
			NotFound(w, r)
			return
		}
		// A ServeMux below h reuses these buffers, which would change r's path values under the caller.
		r2.matches = clonePathValues(r.matches)
		r2.otherValues = clonePathValues(r.otherValues)
		r2.match = matchScratch{}
		h.ServeHTTP(w, r2)
	})
}
