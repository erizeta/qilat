package qilat

import (
	"bufio"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"sync"

	"github.com/erizeta/qilat/internal/bytesconv"
	"github.com/erizeta/qilat/internal/exchange"
)

// FromStd adapts a [net/http.Handler] to a [Handler]. Each request allocates a [net/http.Request] with its URL
// and header map; like net/http, the request may be kept after the handler returns, but its Body may not.
// The ResponseWriter implements [net/http.Flusher], FlushError, and on HTTP/1.x [net/http.Hijacker].
func FromStd(h http.Handler) Handler {
	if t, ok := h.(toStdHandler); ok {
		return t.h
	}
	return fromStdHandler{h}
}

type fromStdHandler struct{ h http.Handler }

func (f fromStdHandler) ServeHTTP(w ResponseWriter, r *Request) {
	r.live()
	req, body, err := newStdRequest(r)
	if err != nil {
		Error(w, "400 Bad Request", 400)
		return
	}
	sw := acquireStdWriter(w)
	defer func() {
		body.close()
		sw.release()
		// net/http handlers abort with their own sentinel, which qilat would otherwise log as a panic.
		if v := recover(); v != nil {
			if v == http.ErrAbortHandler { //nolint:errorlint // panic values are compared, not wrapped
				v = ErrAbortHandler
			}
			panic(v)
		}
	}()
	var rw http.ResponseWriter = sw
	if _, ok := w.(Hijacker); ok && strings.HasPrefix(r.proto, "HTTP/1.") {
		rw = stdHijackWriter{sw}
	}
	f.h.ServeHTTP(rw, req)
	sw.finish()
}

// newStdRequest builds a server-side net/http request the way net/http would have parsed r.
func newStdRequest(r *Request) (*http.Request, *stdBody, error) {
	target := r.uri.String()
	var u *url.URL
	if r.uri.form == formAuthority {
		u = &url.URL{Host: target}
	} else {
		var err error
		if u, err = url.ParseRequestURI(target); err != nil {
			return nil, nil, err
		}
	}
	host := r.uri.Host()
	if len(host) == 0 {
		host = r.host
	}
	req := http.Request{
		Method:     r.method,
		URL:        u,
		Proto:      r.proto,
		Header:     stdRequestHeader(&r.header),
		Host:       string(host),
		RequestURI: target,
		TLS:        r.tls,
		Pattern:    r.pattern,
	}
	req.ProtoMajor, req.ProtoMinor, _ = http.ParseHTTPVersion(r.proto)
	if r.remoteAddr != nil {
		req.RemoteAddr = r.remoteAddr.String()
	}
	var body *stdBody
	if _, ok := r.Body.(noBody); ok {
		req.Body = http.NoBody
	} else {
		body = &stdBody{r: r.Body}
		req.Body = body
		req.ContentLength = r.contentLength
		if req.ContentLength < 0 && req.ProtoMajor == 1 {
			req.TransferEncoding = []string{"chunked"}
		}
	}
	for _, v := range r.matches {
		req.SetPathValue(v.name, string(v.value))
	}
	for _, v := range r.otherValues {
		req.SetPathValue(v.name, string(v.value))
	}
	return req.WithContext(r.Context()), body, nil
}

// stdRequestHeader copies h into a net/http header with canonical keys. All values share one string.
func stdRequestHeader(h *Header) http.Header {
	n := len(h.fields)
	hdr := make(http.Header, n)
	if n == 0 {
		return hdr
	}
	size := 0
	for i := range h.fields {
		size += len(h.fields[i].value)
	}
	var sb strings.Builder
	sb.Grow(size)
	for i := range h.fields {
		sb.Write(h.fields[i].value)
	}
	all := sb.String()
	// Each key's slice has capacity 1, so appending to it never overwrites the next key's value.
	vals := make([]string, n)
	var kbuf [64]byte
	off := 0
	for i := range h.fields {
		f := &h.fields[i]
		vals[i] = all[off : off+len(f.value)]
		off += len(f.value)
		key := canonicalKey(kbuf[:0], f.name)
		if vs, ok := hdr[key]; ok {
			hdr[key] = append(vs, vals[i])
		} else {
			hdr[key] = vals[i : i+1 : i+1]
		}
	}
	return hdr
}

// stdBody fails reads once the handler has returned, since the qilat body behind it is reused.
type stdBody struct {
	r      io.Reader
	closed bool
}

func (b *stdBody) Read(p []byte) (int, error) {
	if b.closed {
		return 0, http.ErrBodyReadAfterClose
	}
	return b.r.Read(p)
}

func (b *stdBody) Close() error {
	b.closed = true
	return nil
}

func (b *stdBody) close() {
	if b != nil {
		b.closed = true
		b.r = nil
	}
}

// sniffLen is how much of the body net/http looks at to detect a missing Content-Type.
const sniffLen = 512

// stdWriter is the net/http ResponseWriter given to handlers adapted by FromStd. Its header map is the source
// of truth until WriteHeader, when it is copied into the qilat header, as net/http snapshots it.
type stdWriter struct {
	w           ResponseWriter
	header      http.Header
	wroteHeader bool
	hijacked    bool
	sniff       bool // Content-Type is still to be detected from sniffBuf
	sniffBuf    []byte
	keys        []string
}

var stdWriterPool = sync.Pool{New: func() any { return &stdWriter{header: make(http.Header)} }}

func acquireStdWriter(w ResponseWriter) *stdWriter {
	sw := stdWriterPool.Get().(*stdWriter)
	sw.w = w
	// Show the handler what outer qilat middleware already set.
	var kbuf [64]byte
	for name, value := range w.Header().All() {
		key := canonicalKey(kbuf[:0], name)
		sw.header[key] = append(sw.header[key], string(value))
	}
	return sw
}

func (sw *stdWriter) release() {
	if len(sw.header) > 64 {
		sw.header = make(http.Header)
	} else {
		clear(sw.header)
	}
	sw.w = nil
	sw.wroteHeader, sw.hijacked, sw.sniff = false, false, false
	sw.sniffBuf = sw.sniffBuf[:0]
	stdWriterPool.Put(sw)
}

func (sw *stdWriter) Header() http.Header { return sw.header }

func (sw *stdWriter) WriteHeader(code int) {
	if sw.hijacked || sw.wroteHeader {
		return
	}
	sw.syncHeader()
	sw.w.WriteHeader(code)
	if code < 200 && code != 101 {
		return
	}
	sw.wroteHeader = true
	_, haveType := sw.header["Content-Type"]
	sw.sniff = !haveType && sw.header.Get("Content-Encoding") == "" && sw.header.Get("Transfer-Encoding") == "" &&
		code != 204 && code != 304 && code >= 200
}

// syncHeader replaces the qilat header with the handler's map. Keys that are not tokens, including the
// "Trailer:" prefixed ones, are skipped like net/http does. Keys are sorted, as net/http writes them.
func (sw *stdWriter) syncHeader() {
	h := sw.w.Header()
	h.Reset()
	keys := sw.keys[:0]
	for k := range sw.header {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	for _, k := range keys {
		if !validName(k) {
			continue
		}
		for _, v := range sw.header[k] {
			h.Add(k, v)
		}
	}
	clear(keys)
	sw.keys = keys[:0]
}

func (sw *stdWriter) Write(p []byte) (int, error) {
	if sw.hijacked {
		return 0, http.ErrHijacked
	}
	if !sw.wroteHeader {
		sw.WriteHeader(200)
	}
	if !sw.sniff {
		return sw.w.Write(p)
	}
	n := min(len(p), sniffLen-len(sw.sniffBuf))
	sw.sniffBuf = append(sw.sniffBuf, p[:n]...)
	if len(sw.sniffBuf) < sniffLen {
		return n, nil
	}
	if err := sw.endSniff(); err != nil {
		return 0, err
	}
	if n == len(p) {
		return n, nil
	}
	m, err := sw.w.Write(p[n:])
	return n + m, err
}

// endSniff sets Content-Type from the buffered start of the body and sends that start.
func (sw *stdWriter) endSniff() error {
	sw.sniff = false
	if len(sw.sniffBuf) == 0 {
		return nil
	}
	sw.w.Header().Set("content-type", http.DetectContentType(sw.sniffBuf))
	_, err := sw.w.Write(sw.sniffBuf)
	sw.sniffBuf = sw.sniffBuf[:0]
	return err
}

// FlushError flushes like Flush and reports the error. [net/http.ResponseController] uses it.
func (sw *stdWriter) FlushError() error {
	if sw.hijacked {
		return http.ErrHijacked
	}
	if !sw.wroteHeader {
		sw.WriteHeader(200)
	}
	if sw.sniff {
		if err := sw.endSniff(); err != nil {
			return err
		}
	}
	return sw.w.Flush()
}

func (sw *stdWriter) Flush() { _ = sw.FlushError() }

// ReadFrom lets io.Copy reach the qilat writer's ReadFrom, which uses sendfile on HTTP/1.1, after the first
// bytes have gone through Write for sniffing, as net/http does.
func (sw *stdWriter) ReadFrom(src io.Reader) (int64, error) {
	rf, ok := sw.w.(io.ReaderFrom)
	if !ok || sw.hijacked {
		return copyBuffered(sw, src)
	}
	var n int64
	if !sw.wroteHeader || sw.sniff {
		n0, err := copyBuffered(sw, io.LimitReader(src, sniffLen))
		n += n0
		if err != nil || n0 < sniffLen {
			return n, err
		}
	}
	n1, err := rf.ReadFrom(src)
	return n + n1, err
}

func (sw *stdWriter) finish() {
	if sw.hijacked {
		return
	}
	if !sw.wroteHeader {
		sw.WriteHeader(200)
	}
	if sw.sniff {
		_ = sw.endSniff()
	}
	sw.setTrailers()
}

// setTrailers sends the values of trailers declared in the Trailer header and of "Trailer:" prefixed keys.
func (sw *stdWriter) setTrailers() {
	ts, ok := sw.w.(TrailerSetter)
	if !ok {
		return
	}
	for k, vs := range sw.header {
		if name, found := strings.CutPrefix(k, http.TrailerPrefix); found {
			addStdValues(ts.Trailer(), name, vs)
		}
	}
	for name, value := range sw.w.Header().All() {
		if string(name) != "trailer" {
			continue
		}
		for t := range strings.SplitSeq(string(value), ",") {
			if t = http.CanonicalHeaderKey(strings.TrimSpace(t)); t != "" {
				addStdValues(ts.Trailer(), t, sw.header[t])
			}
		}
	}
}

func addStdValues(h *Header, name string, vs []string) {
	if !validName(name) {
		return
	}
	for _, v := range vs {
		h.Add(name, v)
	}
}

type stdHijackWriter struct{ *stdWriter }

func (h stdHijackWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	if h.hijacked {
		return nil, nil, http.ErrHijacked
	}
	hj, ok := h.w.(Hijacker)
	if !ok {
		return nil, nil, http.ErrNotSupported
	}
	c, rw, err := hj.Hijack()
	if err == nil {
		h.hijacked = true
	}
	return c, rw, err
}

// ToStd adapts a [Handler] to a [net/http.Handler], so qilat handlers can run on a net/http server.
// Path values set by [net/http.ServeMux] are copied to the request.
func ToStd(h Handler) http.Handler {
	if f, ok := h.(fromStdHandler); ok {
		return f.h
	}
	return toStdHandler{h}
}

type toStdHandler struct{ h Handler }

// toStdConfig gives ToStd exchanges the defaults; the handler is called directly, not through Serve,
// so panics reach net/http unchanged.
var toStdConfig exchangeConfig

func (t toStdHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	x := acquireExchange(&toStdConfig)
	s := stdSinkPool.Get().(*stdSink)
	s.w = w
	defer func() {
		x.Release()
		s.w = nil
		stdSinkPool.Put(s)
		if v := recover(); v != nil {
			if v == ErrAbortHandler { //nolint:errorlint // panic values are compared, not wrapped
				v = http.ErrAbortHandler
			}
			panic(v)
		}
	}()
	if err := fillFromStd(x, r); err != nil {
		http.Error(w, "400 Bad Request", http.StatusBadRequest)
		return
	}
	x.resp.init(s, &x.req)
	t.h.ServeHTTP(&x.resp, &x.req)
	// A short body or a write error leaves net/http with a Content-Length mismatch, so it closes the connection.
	_ = x.resp.finish()
}

func fillFromStd(x *serverExchange, r *http.Request) error {
	if err := x.SetMethod(bytesconv.S2B(r.Method)); err != nil {
		return err
	}
	if err := x.SetTarget(bytesconv.S2B(stdTarget(r))); err != nil {
		return err
	}
	if err := x.SetHost(bytesconv.S2B(r.Host)); err != nil {
		return err
	}
	x.SetProto(r.ProtoMajor, r.ProtoMinor)
	for k, vs := range r.Header {
		for _, v := range vs {
			if err := x.AddHeader(bytesconv.S2B(k), bytesconv.S2B(v)); err != nil {
				return err
			}
		}
	}
	body := r.Body
	if body == http.NoBody {
		body = nil
	}
	x.SetBody(body, r.ContentLength)
	var addr net.Addr
	if r.RemoteAddr != "" {
		addr = stdAddr(r.RemoteAddr)
	}
	x.SetConn(addr, r.TLS)
	x.req.baseCtx = r.Context()
	if r.Pattern != "" {
		x.req.pattern = r.Pattern
		for _, name := range stdPatternNames(r.Pattern) {
			if v := r.PathValue(name); v != "" {
				x.req.SetPathValue(name, v)
			}
		}
	}
	return nil
}

// stdTarget rebuilds the request-target from r.URL rather than r.RequestURI, so changes made by
// middleware such as http.StripPrefix are seen. A path left without a leading slash gets one.
func stdTarget(r *http.Request) string {
	if r.Method == "CONNECT" && r.URL.Path == "" {
		return r.URL.Host
	}
	t := r.URL.RequestURI()
	if t[0] != '/' && t != "*" {
		t = "/" + t
	}
	return t
}

var stdPatterns sync.Map // pattern string → []string of wildcard names

// stdPatternNames returns the wildcard names of a net/http pattern; its syntax is the same as ServeMux's.
func stdPatternNames(p string) []string {
	if v, ok := stdPatterns.Load(p); ok {
		return v.([]string)
	}
	var names []string
	if pat, err := parsePattern(p); err == nil {
		for _, n := range pat.names {
			if n != "" {
				names = append(names, n)
			}
		}
	}
	stdPatterns.Store(p, names)
	return names
}

type stdAddr string

func (a stdAddr) Network() string { return "tcp" }
func (a stdAddr) String() string  { return string(a) }

// stdSink writes a qilat response to a net/http ResponseWriter.
type stdSink struct{ w http.ResponseWriter }

var stdSinkPool = sync.Pool{New: func() any { return new(stdSink) }}

var (
	_ exchange.ResponseSink      = (*stdSink)(nil)
	_ exchange.InformationalSink = (*stdSink)(nil)
	_ exchange.TrailerSink       = (*stdSink)(nil)
	_ exchange.Hijacker          = (*stdSink)(nil)
)

func (s *stdSink) WriteHeader(status int, h exchange.HeaderList, contentLength int64) error {
	copyToStdHeader(s.w.Header(), h)
	s.w.WriteHeader(status)
	// The core leaves the length unknown for streaming and for trailers. Sending the header now stops
	// net/http from adding a Content-Length of its own when the handler ends, which would drop trailers.
	if contentLength < 0 {
		_ = http.NewResponseController(s.w).Flush()
	}
	return nil
}

func (s *stdSink) WriteInformational(status int, h exchange.HeaderList) error {
	copyToStdHeader(s.w.Header(), h)
	s.w.WriteHeader(status)
	return nil
}

func (s *stdSink) Write(p []byte) (int, error) { return s.w.Write(p) }

func (s *stdSink) Flush() error { return http.NewResponseController(s.w).Flush() }

func (s *stdSink) Finish() error { return nil }

func (s *stdSink) FinishTrailer(trailer exchange.HeaderList) error {
	hdr := s.w.Header()
	var kbuf [64]byte
	for i := range trailer.Len() {
		name, value := trailer.At(i)
		key := http.TrailerPrefix + canonicalKey(kbuf[:0], name)
		hdr[key] = append(hdr[key], string(value))
	}
	return nil
}

func (s *stdSink) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	c, rw, err := http.NewResponseController(s.w).Hijack()
	if errors.Is(err, http.ErrNotSupported) {
		err = ErrNotSupported
	}
	return c, rw, err
}

// copyToStdHeader replaces the keys present in h, keeping the others, which outer net/http middleware may have set.
func copyToStdHeader(dst http.Header, h exchange.HeaderList) {
	var kbuf [64]byte
	for i := range h.Len() {
		name, _ := h.At(i)
		delete(dst, canonicalKey(kbuf[:0], name))
	}
	for i := range h.Len() {
		name, value := h.At(i)
		key := canonicalKey(kbuf[:0], name)
		dst[key] = append(dst[key], string(value))
	}
}

// canonicalKey returns the net/http form of a lowercase header name, such as "Content-Type",
// without allocating for common names. buf is scratch space.
func canonicalKey(buf, name []byte) string {
	upper := true
	for _, c := range name {
		if upper && 'a' <= c && c <= 'z' {
			c -= 'a' - 'A'
		}
		buf = append(buf, c)
		upper = c == '-'
	}
	if s, ok := commonHeader[string(buf)]; ok {
		return s
	}
	return string(buf)
}

var commonHeader = func() map[string]string {
	m := make(map[string]string)
	for _, v := range []string{
		"Accept", "Accept-Charset", "Accept-Encoding", "Accept-Language", "Accept-Ranges", "Allow",
		"Authorization", "Cache-Control", "Connection", "Content-Disposition", "Content-Encoding",
		"Content-Language", "Content-Length", "Content-Location", "Content-Range", "Content-Type", "Cookie",
		"Date", "Etag", "Expect", "Expires", "From", "Host", "If-Match", "If-Modified-Since", "If-None-Match",
		"If-Range", "If-Unmodified-Since", "Last-Modified", "Link", "Location", "Max-Forwards", "Origin",
		"Pragma", "Priority", "Proxy-Authorization", "Range", "Referer", "Retry-After", "Sec-Fetch-Dest",
		"Sec-Fetch-Mode", "Sec-Fetch-Site", "Sec-Fetch-User", "Server", "Set-Cookie", "Te", "Trailer",
		"Transfer-Encoding", "Upgrade", "Upgrade-Insecure-Requests", "User-Agent", "Vary", "Via",
		"Www-Authenticate", "X-Forwarded-For", "X-Forwarded-Proto", "X-Real-Ip", "X-Request-Id",
		"Access-Control-Request-Method", "Access-Control-Request-Headers", "Alt-Svc",
	} {
		m[v] = v
	}
	return m
}()

type writerOnly struct{ io.Writer }

type readerOnly struct{ io.Reader }

var copyBufPool = sync.Pool{New: func() any { b := make([]byte, 32<<10); return &b }}

// copyBuffered is io.Copy with a pooled buffer. It hides dst's ReadFrom and src's WriteTo so neither
// calls back into the caller or allocates a buffer of its own.
func copyBuffered(dst io.Writer, src io.Reader) (int64, error) {
	buf := copyBufPool.Get().(*[]byte)
	defer copyBufPool.Put(buf)
	return io.CopyBuffer(writerOnly{dst}, readerOnly{src}, *buf)
}
