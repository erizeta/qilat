package qilat

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"strconv"

	"github.com/erizeta/qilat/internal/bytesconv"
	"github.com/erizeta/qilat/internal/exchange"
)

// Request is an HTTP request received by a server. It is pooled: the Request and every byte slice it
// returns are valid only until ServeHTTP returns. Use [Request.Clone] to keep a request longer.
type Request struct {
	// Header holds the request header fields. The Host header is available from [Request.Host] instead.
	Header *Header
	// Body is the request body. It is never nil and returns io.EOF immediately when there is no body.
	Body io.Reader

	header        Header
	method        string
	uri           URI
	host          []byte
	contentLength int64
	proto         string
	remoteAddr    net.Addr
	tls           *tls.ConnectionState
	baseCtx       context.Context
	ctx           context.Context // set by WithContext and Clone
	st            *requestState
	dead          bool

	pattern     string
	pat         *pattern
	matches     []pathValue // values of pat's wildcards, in pattern order
	otherValues []pathValue // set by SetPathValue for names pat does not have
	match       matchScratch
}

// pathValue owns its bytes so values survive later changes to the request URI.
type pathValue struct {
	name  string
	value []byte
}

// Method returns the request method, such as "GET".
func (r *Request) Method() string {
	r.live()
	return r.method
}

// URI returns the parsed request-target.
func (r *Request) URI() *URI {
	r.live()
	return &r.uri
}

// Path returns the percent-decoded request path. See [URI.Path].
func (r *Request) Path() []byte {
	r.live()
	return r.uri.Path()
}

// Host returns the Host header (HTTP/1.1) or :authority pseudo-header (HTTP/2, HTTP/3).
func (r *Request) Host() []byte {
	r.live()
	return r.host
}

// ContentLength returns the declared body length, or -1 if it is unknown.
func (r *Request) ContentLength() int64 {
	r.live()
	return r.contentLength
}

// Proto returns the protocol version: "HTTP/1.0", "HTTP/1.1", "HTTP/2.0", or "HTTP/3.0".
func (r *Request) Proto() string {
	r.live()
	return r.proto
}

// RemoteAddr returns the network address of the client.
func (r *Request) RemoteAddr() net.Addr {
	r.live()
	return r.remoteAddr
}

// TLS returns the TLS connection state, or nil for unencrypted connections.
func (r *Request) TLS() *tls.ConnectionState {
	r.live()
	return r.tls
}

// Context returns the request context. It is canceled when the client disconnects or ServeHTTP returns.
// The first call allocates a small context that is never reused, so it is safe to pass to goroutines.
func (r *Request) Context() context.Context {
	r.live()
	if r.ctx != nil {
		return r.ctx
	}
	return r.st.context(r.baseCtx)
}

// WithContext returns a shallow copy of r with its context changed to ctx. The copy shares r's header,
// URI, and body, and has the same lifetime as r. It panics if ctx is nil.
func (r *Request) WithContext(ctx context.Context) *Request {
	r.live()
	if ctx == nil {
		panic("qilat: nil context")
	}
	r2 := new(Request)
	*r2 = *r
	r2.ctx = ctx
	return r2
}

// Clone returns a deep copy of r with context ctx that stays valid after the handler returns.
// The body is not copied: the clone's Body is empty. It panics if ctx is nil.
func (r *Request) Clone(ctx context.Context) *Request {
	r.live()
	if ctx == nil {
		panic("qilat: nil context")
	}
	c := &Request{
		Body:          noBody{},
		method:        r.method,
		host:          append([]byte(nil), r.host...),
		contentLength: r.contentLength,
		proto:         r.proto,
		remoteAddr:    r.remoteAddr,
		tls:           r.tls,
		baseCtx:       ctx,
		ctx:           ctx,
		st:            new(requestState),
		pattern:       r.pattern,
		pat:           r.pat,
		matches:       clonePathValues(r.matches),
		otherValues:   clonePathValues(r.otherValues),
	}
	c.Header = &c.header
	r.Header.CopyTo(&c.header)
	r.uri.CopyTo(&c.uri)
	return c
}

// Pattern returns the [ServeMux] pattern that matched the request, or "" if none did.
func (r *Request) Pattern() string {
	r.live()
	return r.pattern
}

// PathValue returns a copy of the value of the named wildcard in the [ServeMux] pattern that matched
// the request, or a value set by [Request.SetPathValue]. It returns "" if there is no such value.
// Like [net/http.Request.PathValue], the value is percent-decoded.
func (r *Request) PathValue(name string) string {
	return string(r.PeekPathValue(name))
}

// PeekPathValue is like [Request.PathValue] but returns the value without copying it, or nil if there is none.
// The result must not be modified and is valid only until ServeHTTP returns.
func (r *Request) PeekPathValue(name string) []byte {
	r.live()
	if v := findPathValue(r.matches, name); v != nil {
		return v.value
	}
	if v := findPathValue(r.otherValues, name); v != nil {
		return v.value
	}
	return nil
}

// SetPathValue sets the value returned by PathValue(name). value is copied and not unescaped.
func (r *Request) SetPathValue(name, value string) {
	r.live()
	v := findPathValue(r.matches, name)
	if v == nil {
		if v = findPathValue(r.otherValues, name); v == nil {
			r.otherValues, v = growPathValues(r.otherValues, name)
		}
	}
	v.value = append(v.value[:0], value...)
}

func findPathValue(vs []pathValue, name string) *pathValue {
	for i := range vs {
		if vs[i].name == name {
			return &vs[i]
		}
	}
	return nil
}

// growPathValues reuses the value buffers of entries left beyond len by an earlier reset.
func growPathValues(vs []pathValue, name string) ([]pathValue, *pathValue) {
	if len(vs) < cap(vs) {
		vs = vs[:len(vs)+1]
	} else {
		vs = append(vs, pathValue{})
	}
	v := &vs[len(vs)-1]
	v.name = name
	v.value = v.value[:0]
	return vs, v
}

// setMatch records the ServeMux match, decoding raw wildcard values into buffers owned by r.
func (r *Request) setMatch(patStr string, pat *pattern, raw [][]byte) {
	r.pattern = patStr
	r.pat = pat
	r.matches = r.matches[:0]
	if pat == nil {
		return
	}
	for i, name := range pat.names {
		if i >= len(raw) {
			break
		}
		var v *pathValue
		r.matches, v = growPathValues(r.matches, name)
		v.value = appendUnescape(v.value, raw[i], false)
	}
}

func clonePathValues(vs []pathValue) []pathValue {
	if len(vs) == 0 {
		return nil
	}
	c := make([]pathValue, len(vs))
	for i, v := range vs {
		c[i] = pathValue{name: v.name, value: append([]byte{}, v.value...)}
	}
	return c
}

func (r *Request) live() {
	if r.dead {
		panic("qilat: Request used after ServeHTTP returned")
	}
}

func (r *Request) init(baseCtx context.Context) {
	if r.st == nil {
		r.st = new(requestState)
	}
	r.Header = &r.header
	r.Body = noBody{}
	r.contentLength = -1
	r.proto = "HTTP/1.1"
	if baseCtx == nil {
		baseCtx = context.Background()
	}
	r.baseCtx = baseCtx
}

func (r *Request) reset() {
	r.header.Reset()
	r.uri.reset()
	r.Header = nil
	r.Body = nil
	r.method = ""
	r.host = r.host[:0]
	r.remoteAddr = nil
	r.tls = nil
	r.baseCtx = nil
	r.ctx = nil
	r.st.reset()
	r.pattern = ""
	r.pat = nil
	r.matches = r.matches[:0]
	r.otherValues = r.otherValues[:0]
}

func (r *Request) poison() {
	r.dead = true
	r.header.dead = true
}

var errInvalidMethod = errors.New("qilat: invalid request method")

func (r *Request) setMethod(m []byte) error {
	if !validName(bytesconv.B2S(m)) {
		return errInvalidMethod
	}
	r.method = internMethod(m)
	return nil
}

// internMethod returns a constant for standard methods so the common case does not allocate.
func internMethod(m []byte) string {
	for _, s := range standardMethods {
		if bytesconv.B2S(m) == s {
			return s
		}
	}
	return string(m)
}

var standardMethods = [...]string{
	"GET", "HEAD", "POST", "PUT", "DELETE", "CONNECT", "OPTIONS", "TRACE", "PATCH",
}

var errInvalidHost = errors.New("qilat: invalid host")

func (r *Request) setHost(h []byte) error {
	for _, c := range h {
		if c <= ' ' || c == 0x7f || c == '/' || c == '?' || c == '#' || c == '@' {
			return errInvalidHost
		}
	}
	r.host = append(r.host[:0], h...)
	return nil
}

func (r *Request) setProto(major, minor int) {
	switch {
	case major == 1 && minor == 1:
		r.proto = "HTTP/1.1"
	case major == 1 && minor == 0:
		r.proto = "HTTP/1.0"
	case major == 2:
		r.proto = "HTTP/2.0"
	case major == 3:
		r.proto = "HTTP/3.0"
	default:
		r.proto = "HTTP/" + strconv.Itoa(major) + "." + strconv.Itoa(minor)
	}
}

// ErrBodyTooLarge is returned when reading a request body that exceeds [Server.MaxRequestBodySize].
var ErrBodyTooLarge = exchange.ErrBodyTooLarge

type noBody struct{}

func (noBody) Read([]byte) (int, error) { return 0, io.EOF }
