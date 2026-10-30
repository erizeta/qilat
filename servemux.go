package qilat

import (
	"bytes"
	"errors"
	"fmt"
	"runtime"
	"slices"
	"strings"
	"sync"
)

// ServeMux is an HTTP request multiplexer with the pattern syntax, precedence, and redirect behavior of
// [net/http.ServeMux] (Go 1.22 and later). Matching a request does not allocate.
//
// Patterns have the form "[METHOD ][HOST]/[PATH]", for example "GET /users/{id}", "example.com/", or
// "/files/{path...}". A wildcard {name} matches one path segment and {name...} the rest of the path; their
// values are available from [Request.PathValue]. {$} matches only the end of the path, and a pattern ending
// in a slash matches every path below it. GET patterns also match HEAD requests.
//
// When several patterns match, the most specific one wins; a pattern with a host beats one without.
// Registering a pattern that conflicts with an existing one (neither is more specific) panics.
//
// Requests whose path is not clean (containing ".", "..", or repeated slashes) are redirected to the clean
// path, and a request for "/dir" is redirected to "/dir/" when only "/dir/" is registered. Both redirects
// use 307 Temporary Redirect, like net/http since Go 1.26. A path that matches no pattern gets 404, and
// one that matches only with another method gets 405 with an Allow header. CONNECT requests are matched
// without cleaning the path or stripping the port.
type ServeMux struct {
	mu    sync.RWMutex
	tree  muxTree
	index patternIndex
}

// NewServeMux allocates and returns a new [ServeMux].
func NewServeMux() *ServeMux { return new(ServeMux) }

// Handle registers handler for pattern. It panics if pattern is invalid or conflicts with a registered pattern.
func (mux *ServeMux) Handle(pattern string, handler Handler) {
	if err := mux.register(pattern, handler); err != nil {
		panic(err)
	}
}

// HandleFunc registers handler for pattern. It panics like [ServeMux.Handle].
func (mux *ServeMux) HandleFunc(pattern string, handler func(ResponseWriter, *Request)) {
	if handler == nil {
		panic(errNilHandler)
	}
	if err := mux.register(pattern, HandlerFunc(handler)); err != nil {
		panic(err)
	}
}

var errNilHandler = errors.New("qilat: nil handler")

// register must be called directly by Handle or HandleFunc so the caller's location is recorded.
func (mux *ServeMux) register(patstr string, handler Handler) error {
	if patstr == "" {
		return errors.New("qilat: invalid pattern")
	}
	if handler == nil {
		return errNilHandler
	}
	if f, ok := handler.(HandlerFunc); ok && f == nil {
		return errNilHandler
	}
	pat, err := parsePattern(patstr)
	if err != nil {
		return fmt.Errorf("qilat: parsing %q: %w", patstr, err)
	}
	if _, file, line, ok := runtime.Caller(2); ok {
		pat.loc = fmt.Sprintf("%s:%d", file, line)
	} else {
		pat.loc = "unknown location"
	}

	mux.mu.Lock()
	defer mux.mu.Unlock()
	err = mux.index.candidates(pat, func(q *pattern) error {
		if pat.conflictsWith(q) {
			return fmt.Errorf("qilat: pattern %q (registered at %s) conflicts with pattern %q (registered at %s):\n%s",
				pat, pat.loc, q, q.loc, describeConflict(pat, q))
		}
		return nil
	})
	if err != nil {
		return err
	}
	mux.tree.add(pat, handler)
	mux.index.add(pat)
	return nil
}

// ServeHTTP dispatches the request to the handler whose pattern most closely matches it,
// setting [Request.Pattern] and the values returned by [Request.PathValue].
func (mux *ServeMux) ServeHTTP(w ResponseWriter, r *Request) {
	r.live()
	if r.uri.form == formAsterisk {
		if r.proto == "HTTP/1.1" {
			w.Header().Set("connection", "close")
		}
		w.WriteHeader(400)
		return
	}
	h, patStr, pat, matches := mux.findHandler(r)
	r.setMatch(patStr, pat, matches)
	h.ServeHTTP(w, r)
}

// Handler returns the handler to use for r and the pattern that matched it. It never returns nil: when nothing
// matches, it returns a handler that replies 404 or 405 and an empty pattern, and when the request must be
// redirected, a redirecting handler and the pattern that will match after the redirect.
// Handler does not set path values, so r.PathValue keeps returning what it did before.
func (mux *ServeMux) Handler(r *Request) (h Handler, pattern string) {
	r.live()
	h, pattern, _, _ = mux.findHandler(r)
	return h, pattern
}

func (mux *ServeMux) findHandler(r *Request) (Handler, string, *pattern, [][]byte) {
	sc := &r.match
	escaped := escapedPath(r.uri.RawPath())
	// Like net/http, the target's authority takes precedence over the Host header (RFC 9112 section 3.2.2).
	host := r.uri.Host()
	if len(host) == 0 {
		host = r.host
	}
	if r.method == "CONNECT" {
		// The trailing-slash redirect applies to CONNECT, but path cleaning and port stripping do not.
		if _, _, loc := mux.matchOrRedirect(host, r.method, escaped, r.uri.rawQuery, true, sc); loc != "" {
			return redirectHandler(loc), unescapedPath(escaped) + "/", nil, nil
		}
		n, m, _ := mux.matchOrRedirect(host, r.method, escaped, nil, false, sc)
		return mux.result(n, m, host, escaped, sc)
	}

	host = stripHostPort(host)
	path := escaped
	if !isCleanPath(path) {
		path = []byte(cleanPath(string(path)))
	}
	n, m, loc := mux.matchOrRedirect(host, r.method, path, r.uri.rawQuery, true, sc)
	if loc != "" {
		return redirectHandler(loc), n.pat.str, nil, nil
	}
	if !bytes.Equal(path, escaped) {
		patStr := ""
		if n != nil {
			patStr = n.pat.str
		}
		return redirectHandler(redirectLocation(path, r.uri.rawQuery)), patStr, nil, nil
	}
	return mux.result(n, m, host, path, sc)
}

func (mux *ServeMux) result(n *muxNode, m [][]byte, host, path []byte, sc *matchScratch) (Handler, string, *pattern, [][]byte) {
	if n != nil {
		return n.handler, n.pat.str, n.pat, m
	}
	if allow := mux.allowedMethods(host, path, sc); len(allow) > 0 {
		return methodNotAllowedHandler(strings.Join(allow, ", ")), "", nil, nil
	}
	return notFoundHandler, "", nil, nil
}

// matchOrRedirect matches the request. When redirect is set and path matches only with a trailing slash
// added, it returns the leaf for that path and the redirect location instead.
func (mux *ServeMux) matchOrRedirect(host []byte, method string, path, rawQuery []byte, redirect bool,
	sc *matchScratch) (*muxNode, [][]byte, string) {
	mux.mu.RLock()
	defer mux.mu.RUnlock()
	n, m := mux.tree.match(host, method, path, sc.raw[:0], sc)
	if cap(m) > cap(sc.raw) {
		sc.raw = m[:0]
	}
	if redirect && len(path) > 0 && path[len(path)-1] != '/' && !exactMatch(n, path) {
		sc.path = append(append(sc.path[:0], path...), '/')
		n2, m2 := mux.tree.match(host, method, sc.path, sc.slashed[:0], sc)
		if cap(m2) > cap(sc.slashed) {
			sc.slashed = m2[:0]
		}
		if exactMatch(n2, sc.path) {
			return n2, nil, redirectLocation(sc.path, rawQuery)
		}
	}
	return n, m, ""
}

func (mux *ServeMux) allowedMethods(host, path []byte, sc *matchScratch) []string {
	mux.mu.RLock()
	defer mux.mu.RUnlock()
	ms := mux.tree.allowedMethods(host, path, sc)
	// A trailing-slash redirect would also apply with any of these methods.
	if len(path) == 0 || path[len(path)-1] != '/' {
		sc.path = append(append(sc.path[:0], path...), '/')
		for _, m := range mux.tree.allowedMethods(host, sc.path, sc) {
			if !slices.Contains(ms, m) {
				ms = append(ms, m)
			}
		}
		slices.Sort(ms)
	}
	return ms
}

// isCleanPath reports whether cleanPath(p) == p, without allocating.
func isCleanPath(p []byte) bool {
	if len(p) == 0 || p[0] != '/' {
		return false
	}
	for i := 0; i < len(p); i++ {
		if p[i] != '/' {
			continue
		}
		next := p[i+1:]
		if j := bytes.IndexByte(next, '/'); j >= 0 {
			next = next[:j]
		} else if len(next) == 0 {
			return true // trailing slash
		}
		if len(next) == 0 || string(next) == "." || string(next) == ".." {
			return false
		}
	}
	return true
}

// stripHostPort removes a ":port" suffix the way net.SplitHostPort would, returning h unchanged if it fails.
func stripHostPort(h []byte) []byte {
	i := bytes.LastIndexByte(h, ':')
	if i < 0 {
		return h
	}
	host, j, k := h[:i], 0, 0
	if h[0] == '[' {
		end := bytes.IndexByte(h, ']')
		if end < 0 || end+1 != i {
			return h
		}
		host, j, k = h[1:end], 1, end+1
	} else if bytes.IndexByte(host, ':') >= 0 {
		return h
	}
	if bytes.IndexByte(h[j:], '[') >= 0 || bytes.IndexByte(h[k:], ']') >= 0 {
		return h
	}
	return host
}

func unescapedPath(escaped []byte) string { return string(appendUnescape(nil, escaped, false)) }

// escapedPath mirrors net/url's EscapedPath: a path holding bytes that are not allowed unescaped is
// escaped again with net/url's stricter rules. Existing escapes are kept, so %2F never becomes a separator.
func escapedPath(p []byte) []byte {
	valid := true
	for _, c := range p {
		valid = valid && pathBytes[c]&pathPchar != 0
	}
	if valid {
		return p
	}
	b := make([]byte, 0, len(p)+16)
	for _, c := range p {
		if pathBytes[c]&pathUnescaped != 0 {
			b = append(b, c)
		} else {
			b = append(b, '%', "0123456789ABCDEF"[c>>4], "0123456789ABCDEF"[c&0xf])
		}
	}
	return b
}

const (
	pathPchar     = 1 << iota // allowed unescaped in an escaped path: RFC 3986 pchar, '/', and '%'
	pathUnescaped             // left alone when net/url escapes a path, plus '%' to keep existing escapes
)

var pathBytes = func() (t [256]uint8) {
	for c := range 256 {
		alnum := isAlpha(byte(c)) || '0' <= c && c <= '9'
		if alnum || strings.IndexByte("-._~!$&'()*+,;=:@[]%/", byte(c)) >= 0 {
			t[c] |= pathPchar
		}
		if alnum || strings.IndexByte("-_.~$&+,/:;=@%", byte(c)) >= 0 {
			t[c] |= pathUnescaped
		}
	}
	return t
}()

func redirectLocation(path, rawQuery []byte) string {
	if len(rawQuery) == 0 {
		return string(path)
	}
	return string(path) + "?" + string(rawQuery)
}

// hexEscapeNonASCII escapes bytes outside ASCII for a Location header, with lowercase hex like net/http.
func hexEscapeNonASCII(s string) string {
	n := 0
	for i := 0; i < len(s); i++ {
		if s[i] >= 0x80 {
			n++
		}
	}
	if n == 0 {
		return s
	}
	b := make([]byte, 0, len(s)+2*n)
	for i := 0; i < len(s); i++ {
		if c := s[i]; c >= 0x80 {
			b = append(b, '%', "0123456789abcdef"[c>>4], "0123456789abcdef"[c&0xf])
		} else {
			b = append(b, c)
		}
	}
	return string(b)
}

func redirectHandler(loc string) Handler {
	return HandlerFunc(func(w ResponseWriter, r *Request) {
		h := w.Header()
		hadCT := h.Has("content-type")
		h.Set("location", hexEscapeNonASCII(loc))
		if !hadCT && (r.method == "GET" || r.method == "HEAD") {
			h.Set("content-type", "text/html; charset=utf-8")
		}
		w.WriteHeader(307)
		// Old user agents may not follow a 307 without a body; net/http sends one for GET only.
		if !hadCT && r.method == "GET" {
			_, _ = w.WriteString("<a href=\"" + htmlEscaper.Replace(loc) + "\">Temporary Redirect</a>.\n\n")
		}
	})
}

var htmlEscaper = strings.NewReplacer("&", "&amp;", "<", "&lt;", ">", "&gt;", `"`, "&#34;", "'", "&#39;")

var notFoundHandler = HandlerFunc(func(w ResponseWriter, _ *Request) {
	writeError(w, "404 page not found", 404)
})

func methodNotAllowedHandler(allow string) Handler {
	return HandlerFunc(func(w ResponseWriter, _ *Request) {
		w.Header().Set("allow", allow)
		writeError(w, "Method Not Allowed", 405)
	})
}

// writeError replies with a plain-text error, like net/http.Error.
func writeError(w ResponseWriter, msg string, code int) {
	h := w.Header()
	h.Del("content-length")
	h.Set("content-type", "text/plain; charset=utf-8")
	h.Set("x-content-type-options", "nosniff")
	w.WriteHeader(code)
	_, _ = w.WriteString(msg)
	_, _ = w.WriteString("\n")
}
