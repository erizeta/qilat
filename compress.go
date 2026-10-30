package qilat

import (
	"bufio"
	"compress/gzip"
	"fmt"
	"io"
	"net"
	"strconv"
	"sync"

	"github.com/erizeta/qilat/internal/bytesconv"
)

// DefaultCompressMinSize is the smallest response body [Compress] compresses when MinSize is zero.
const DefaultCompressMinSize = 1024

// CompressConfig configures [CompressWith]. The zero value compresses with gzip at the default level.
type CompressConfig struct {
	// Level is the gzip level, from gzip.HuffmanOnly to gzip.BestCompression. Zero means gzip.DefaultCompression.
	Level int
	// MinSize is the smallest body to compress; zero means DefaultCompressMinSize. Smaller bodies are not worth it.
	MinSize int
	// Encoders adds content codings such as "zstd" or "br". When the client accepts several codings equally,
	// they are preferred in this order, then gzip.
	Encoders []Encoder
}

// Encoder is a content coding for [CompressWith].
type Encoder struct {
	// Name is the content coding, as sent in Content-Encoding, such as "zstd".
	Name string
	// New returns a writer that compresses to w. Writers are pooled and reused with Reset.
	New func(w io.Writer) CompressWriter
}

// CompressWriter is a reusable compressing writer, such as *gzip.Writer, *zstd.Encoder from
// github.com/klauspost/compress, or *brotli.Writer from github.com/andybalholm/brotli.
type CompressWriter interface {
	io.WriteCloser
	Flush() error
	Reset(w io.Writer)
}

// Compress returns middleware that compresses response bodies with gzip at level. See [CompressWith].
func Compress(level int) func(Handler) Handler {
	return CompressWith(CompressConfig{Level: level})
}

// CompressWith returns middleware that compresses response bodies with the coding the client prefers in
// Accept-Encoding. It skips small and already encoded bodies, Range requests, partial content, server-sent
// events, Cache-Control: no-transform, and types that do not compress, such as images.
// Compressible responses get Vary: Accept-Encoding, and a strong ETag becomes weak. It panics on an invalid Level.
func CompressWith(cfg CompressConfig) func(Handler) Handler {
	level := cfg.Level
	if level == 0 {
		level = gzip.DefaultCompression
	}
	if _, err := gzip.NewWriterLevel(io.Discard, level); err != nil {
		panic(fmt.Sprintf("qilat: invalid gzip level %d", cfg.Level))
	}
	c := &compressor{minSize: cfg.MinSize}
	if c.minSize <= 0 {
		c.minSize = DefaultCompressMinSize
	}
	for _, e := range cfg.Encoders {
		if !validName(e.Name) || e.New == nil {
			panic(fmt.Sprintf("qilat: invalid encoder %q", e.Name))
		}
		c.codings = append(c.codings, newCoding(e.Name, e.New))
	}
	c.codings = append(c.codings, newCoding("gzip", func(w io.Writer) CompressWriter {
		zw, _ := gzip.NewWriterLevel(w, level)
		return zw
	}))
	return c.wrap
}

type compressor struct {
	minSize int
	codings []*coding // in server preference order
}

type coding struct {
	name string
	pool sync.Pool // of *pooledEncoder
}

// pooledEncoder writes through dst, so a pooled encoder never holds on to a finished response.
type pooledEncoder struct {
	enc CompressWriter
	dst forwardWriter
}

type forwardWriter struct{ w io.Writer }

func (f *forwardWriter) Write(p []byte) (int, error) { return f.w.Write(p) }

func newCoding(name string, newWriter func(io.Writer) CompressWriter) *coding {
	c := &coding{name: name}
	c.pool.New = func() any {
		pe := new(pooledEncoder)
		pe.enc = newWriter(&pe.dst)
		return pe
	}
	return c
}

func (c *coding) get(w io.Writer) *pooledEncoder {
	pe := c.pool.Get().(*pooledEncoder)
	pe.dst.w = w
	pe.enc.Reset(&pe.dst)
	return pe
}

func (c *coding) put(pe *pooledEncoder) {
	pe.dst.w = nil
	c.pool.Put(pe)
}

func (c *compressor) wrap(next Handler) Handler {
	return HandlerFunc(func(w ResponseWriter, r *Request) {
		cw := compressWriterPool.Get().(*compressWriter)
		cw.w = w
		cw.minSize = c.minSize
		// A range indexes the uncompressed representation, so it cannot be served compressed.
		if !r.Header.Has("range") {
			cw.coding = c.negotiate(r.Header.Peek("accept-encoding"))
		}
		next.ServeHTTP(cw, r)
		cw.finish()
		cw.release()
	})
}

// negotiate returns the coding with the highest q-value in the Accept-Encoding header ae, or nil.
func (c *compressor) negotiate(ae []byte) *coding {
	if len(ae) == 0 {
		return nil
	}
	star, hasStar := acceptQ(ae, "*")
	var best *coding
	bestQ := 0.0
	for _, cd := range c.codings {
		q, ok := acceptQ(ae, cd.name)
		if !ok && cd.name == "gzip" {
			q, ok = acceptQ(ae, "x-gzip")
		}
		if !ok && hasStar {
			q = star
		}
		if q > bestQ {
			best, bestQ = cd, q
		}
	}
	return best
}

// acceptQ returns the q-value of coding in an Accept-Encoding list, and whether the list names it.
// A missing q means 1, and an invalid one makes the coding unacceptable.
func acceptQ(ae []byte, coding string) (float64, bool) {
	for len(ae) > 0 {
		var elem []byte
		elem, ae, _ = cutByte(ae, ',')
		name, params, _ := cutByte(elem, ';')
		if !equalFold(trimOWS(name), coding) {
			continue
		}
		q := 1.0
		for len(params) > 0 {
			var p []byte
			p, params, _ = cutByte(params, ';')
			p = trimOWS(p)
			if len(p) < 2 || p[0]|0x20 != 'q' || p[1] != '=' {
				continue
			}
			v, err := strconv.ParseFloat(bytesconv.B2S(p[2:]), 64)
			if err != nil || v < 0 || v > 1 {
				v = 0
			}
			q = v
		}
		return q, true
	}
	return 0, false
}

// compressWriter decides whether to compress when the body starts: it buffers up to minSize bytes when the
// length is unknown, so small responses are sent as they are.
type compressWriter struct {
	w           ResponseWriter
	coding      *coding
	minSize     int
	status      int
	wroteHeader bool
	decided     bool
	hijacked    bool
	enc         *pooledEncoder // set while compressing
	buf         []byte
}

var _ interface {
	ResponseWriter
	Hijacker
	TrailerSetter
	io.ReaderFrom
} = (*compressWriter)(nil)

var compressWriterPool = sync.Pool{New: func() any { return new(compressWriter) }}

func (cw *compressWriter) release() {
	buf := cw.buf[:0]
	if cap(buf) > 4*DefaultCompressMinSize {
		buf = nil
	}
	*cw = compressWriter{buf: buf}
	compressWriterPool.Put(cw)
}

func (cw *compressWriter) Header() *Header { return cw.w.Header() }

func (cw *compressWriter) WriteHeader(code int) {
	if cw.wroteHeader || cw.hijacked {
		return
	}
	// Informational responses, and the panic for invalid codes, are the underlying writer's.
	if code < 200 && code != 101 || code > 999 {
		cw.w.WriteHeader(code)
		return
	}
	cw.wroteHeader = true
	cw.status = code
}

func (cw *compressWriter) Write(p []byte) (int, error) {
	if cw.hijacked {
		return 0, ErrHijacked
	}
	if !cw.wroteHeader {
		cw.WriteHeader(200)
	}
	if !cw.decided {
		if cw.mayBuffer() && len(cw.buf)+len(p) < cw.minSize {
			cw.buf = append(cw.buf, p...)
			return len(p), nil
		}
		if err := cw.decide(false); err != nil {
			return 0, err
		}
	}
	if cw.enc != nil {
		return cw.enc.enc.Write(p)
	}
	return cw.w.Write(p)
}

func (cw *compressWriter) WriteString(s string) (int, error) { return cw.Write(bytesconv.S2B(s)) }

func (cw *compressWriter) Flush() error {
	if cw.hijacked {
		return ErrHijacked
	}
	if !cw.wroteHeader {
		cw.WriteHeader(200)
	}
	if !cw.decided {
		if err := cw.decide(false); err != nil {
			return err
		}
	}
	if cw.enc != nil {
		if err := cw.enc.enc.Flush(); err != nil {
			return err
		}
	}
	return cw.w.Flush()
}

// ReadFrom keeps the underlying writer's sendfile path for responses that are not compressed.
func (cw *compressWriter) ReadFrom(src io.Reader) (int64, error) {
	if !cw.hijacked && !cw.decided && !cw.mayBuffer() {
		if !cw.wroteHeader {
			cw.WriteHeader(200)
		}
		if err := cw.decide(false); err != nil {
			return 0, err
		}
	}
	if rf, ok := cw.w.(io.ReaderFrom); ok && cw.decided && cw.enc == nil {
		return rf.ReadFrom(src)
	}
	return copyBuffered(cw, src)
}

func (cw *compressWriter) Trailer() *Header {
	if ts, ok := cw.w.(TrailerSetter); ok {
		return ts.Trailer()
	}
	return new(Header)
}

func (cw *compressWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	hj, ok := cw.w.(Hijacker)
	if !ok {
		return nil, nil, ErrNotSupported
	}
	c, rw, err := hj.Hijack()
	if err == nil {
		cw.hijacked = true
	}
	return c, rw, err
}

func (cw *compressWriter) finish() {
	if cw.hijacked {
		return
	}
	if !cw.wroteHeader {
		cw.WriteHeader(200)
	}
	if !cw.decided {
		_ = cw.decide(true)
	}
	if cw.enc != nil {
		_ = cw.enc.enc.Close()
		cw.coding.put(cw.enc)
		cw.enc = nil
	}
}

// mayBuffer reports whether the response could still be compressed, so its start is worth holding back.
func (cw *compressWriter) mayBuffer() bool {
	return cw.coding != nil && cw.declaredLength() < 0 && cw.compressible()
}

func (cw *compressWriter) declaredLength() int64 {
	if v := cw.w.Header().Peek("content-length"); v != nil {
		if n, ok := bytesconv.ParseUint(v); ok {
			return n
		}
	}
	return -1
}

// compressible reports whether the response is one that compression applies to, whatever the client accepts.
func (cw *compressWriter) compressible() bool {
	s := cw.status
	if s < 200 || s == 204 || s == 206 || s == 304 {
		return false
	}
	h := cw.w.Header()
	if len(h.Peek("content-encoding")) > 0 || h.Has("content-range") ||
		hasToken(h.Peek("cache-control"), "no-transform") {
		return false
	}
	return compressibleType(h.Peek("content-type"))
}

// decide sends the header, compressed or not, followed by any buffered body. final means the handler has
// returned, so the buffer is the whole body.
func (cw *compressWriter) decide(final bool) error {
	cw.decided = true
	h := cw.w.Header()
	if cw.compressible() {
		if !hasToken(h.Peek("vary"), "accept-encoding") && !hasToken(h.Peek("vary"), "*") {
			h.Add("vary", "Accept-Encoding")
		}
		n := cw.declaredLength()
		if cw.coding != nil && (n >= int64(cw.minSize) || n < 0 && (!final || len(cw.buf) >= cw.minSize)) {
			h.Set("content-encoding", cw.coding.name)
			h.Del("content-length")
			// The compressed bytes differ, so a strong validator would be wrong (RFC 9110 section 8.8.1).
			if et := h.Peek("etag"); len(et) > 0 && et[0] == '"' {
				h.Set("etag", "W/"+string(et))
			}
			cw.enc = cw.coding.get(cw.w)
		}
	}
	cw.w.WriteHeader(cw.status)
	if len(cw.buf) == 0 {
		return nil
	}
	var err error
	if cw.enc != nil {
		_, err = cw.enc.enc.Write(cw.buf)
	} else {
		_, err = cw.w.Write(cw.buf)
	}
	cw.buf = cw.buf[:0]
	return err
}

// compressibleType reports whether a media type is text-like. An unknown type is assumed to be.
func compressibleType(ct []byte) bool {
	mt, _, _ := cutByte(ct, ';')
	mt = trimOWS(mt)
	if len(mt) == 0 {
		return true
	}
	var b [64]byte
	if len(mt) > len(b) {
		return false
	}
	lower := lowerInto(b[:0], mt)
	s := bytesconv.B2S(lower)
	switch {
	case s == "text/event-stream":
		return false // events must reach the client as soon as they are flushed
	case len(s) > 5 && s[:5] == "text/":
		return true
	case hasSuffix(s, "+json"), hasSuffix(s, "+xml"):
		return true
	}
	return compressibleTypes[s]
}

var compressibleTypes = map[string]bool{
	"application/json": true, "application/javascript": true, "application/x-javascript": true,
	"application/ecmascript": true, "application/xml": true, "application/x-ndjson": true,
	"application/wasm": true, "application/x-yaml": true, "application/yaml": true, "application/toml": true,
	"application/graphql": true, "application/x-www-form-urlencoded": true, "application/vnd.ms-fontobject": true,
	"image/svg+xml": true, "image/x-icon": true, "image/vnd.microsoft.icon": true, "image/bmp": true,
	"font/ttf": true, "font/otf": true, "font/collection": true,
}

func hasSuffix(s, suffix string) bool {
	return len(s) >= len(suffix) && s[len(s)-len(suffix):] == suffix
}

func lowerInto(dst, b []byte) []byte {
	for _, c := range b {
		dst = append(dst, toLower(c))
	}
	return dst
}

// equalFold reports whether b and s are equal under ASCII case folding.
func equalFold(b []byte, s string) bool {
	if len(b) != len(s) {
		return false
	}
	for i := range len(b) {
		if toLower(b[i]) != toLower(s[i]) {
			return false
		}
	}
	return true
}

func toLower(c byte) byte {
	if 'A' <= c && c <= 'Z' {
		return c + 'a' - 'A'
	}
	return c
}

// hasToken reports whether the comma-separated list v contains token, ignoring case.
func hasToken(v []byte, token string) bool {
	for len(v) > 0 {
		var elem []byte
		elem, v, _ = cutByte(v, ',')
		elem = trimOWS(elem)
		if equalFold(elem, token) {
			return true
		}
	}
	return false
}

func cutByte(b []byte, sep byte) (before, after []byte, found bool) {
	for i, c := range b {
		if c == sep {
			return b[:i], b[i+1:], true
		}
	}
	return b, nil, false
}

func trimOWS(b []byte) []byte {
	for len(b) > 0 && (b[0] == ' ' || b[0] == '\t') {
		b = b[1:]
	}
	for len(b) > 0 && (b[len(b)-1] == ' ' || b[len(b)-1] == '\t') {
		b = b[:len(b)-1]
	}
	return b
}
