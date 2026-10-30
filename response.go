package qilat

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"strconv"

	"github.com/erizeta/qilat/internal/bytesconv"
	"github.com/erizeta/qilat/internal/exchange"
)

// ResponseWriter is used by a [Handler] to construct a response. It must not be used after ServeHTTP returns.
type ResponseWriter interface {
	// Header returns the response header. Content-Length is read when WriteHeader is called, and changes made
	// after the header is sent (on Flush, a large Write, or when ServeHTTP returns) have no effect.
	Header() *Header
	// WriteHeader sends the status code. Only the first call has an effect; it panics on codes outside 100–999.
	WriteHeader(statusCode int)
	// Write sends body bytes, calling WriteHeader(200) first if needed.
	Write(p []byte) (int, error)
	WriteString(s string) (int, error)
	// Flush sends the header and any buffered body bytes to the client.
	Flush() error
}

// Hijacker lets a handler take over the connection. Every qilat ResponseWriter implements it;
// Hijack returns [ErrNotSupported] when the protocol has no connection to hand over (HTTP/2, HTTP/3).
type Hijacker interface {
	Hijack() (net.Conn, *bufio.ReadWriter, error)
}

// TrailerSetter lets a handler send trailer fields after the body. Every qilat ResponseWriter implements it.
// Fields set on the returned Header before ServeHTTP returns are sent as trailers. HTTP/2 always delivers them.
// HTTP/1.1 sends them in a chunked body, so they are dropped when the handler sets Content-Length itself or
// the client speaks HTTP/1.0.
type TrailerSetter interface {
	Trailer() *Header
}

var (
	// ErrNotSupported is returned when the protocol does not support a feature, such as Hijack on HTTP/2.
	ErrNotSupported = errors.New("qilat: feature not supported")
	// ErrHijacked is returned by ResponseWriter methods after the connection has been hijacked.
	ErrHijacked = errors.New("qilat: connection has been hijacked")
	// ErrBodyNotAllowed is returned by Write when the status code does not allow a body.
	ErrBodyNotAllowed = errors.New("qilat: request method or response status code does not allow body")
	// ErrContentLength is returned when the body length differs from a declared Content-Length.
	ErrContentLength = errors.New("qilat: body length does not match declared Content-Length")
)

// responseBufferSize is how much body is buffered before the header is sent, so small responses
// get an exact Content-Length instead of streaming.
const responseBufferSize = 4 << 10

type response struct {
	sink        exchange.ResponseSink
	header      Header
	trailer     Header
	status      int
	wroteHeader bool
	sentHeader  bool
	noBody      bool // the status code forbids a body
	head        bool // HEAD request: count body bytes for Content-Length but send none
	declared    int64
	written     int64
	buf         []byte
	err         error
	hijacked    bool
	dead        bool
}

func (w *response) init(sink exchange.ResponseSink, req *Request) {
	w.sink = sink
	w.head = req.method == "HEAD"
	w.declared = -1
}

func (w *response) Header() *Header {
	w.live()
	return &w.header
}

func (w *response) WriteHeader(code int) {
	w.live()
	if w.wroteHeader {
		return
	}
	if code < 100 || code > 999 {
		panic(fmt.Sprintf("qilat: invalid WriteHeader code %d", code))
	}
	// Like net/http, a 1xx status other than 101 is sent at once with the current header, and is not final.
	if code < 200 && code != 101 {
		if s, ok := w.sink.(exchange.InformationalSink); ok && w.err == nil && !w.hijacked {
			if err := s.WriteInformational(code, (*headerList)(&w.header)); err != nil {
				w.err = err
			}
		}
		return
	}
	w.wroteHeader = true
	w.status = code
	w.noBody = code < 200 || code == 204 || code == 304
	if v := w.header.Peek("content-length"); v != nil {
		if n, ok := bytesconv.ParseUint(v); ok {
			w.declared = n
		} else {
			w.header.Del("content-length")
		}
	}
}

func (w *response) Write(p []byte) (int, error) {
	w.live()
	if w.hijacked {
		return 0, ErrHijacked
	}
	if !w.wroteHeader {
		w.WriteHeader(200)
	}
	if w.err != nil {
		return 0, w.err
	}
	if w.noBody {
		return 0, ErrBodyNotAllowed
	}
	if w.declared >= 0 && w.written+int64(len(p)) > w.declared {
		return 0, ErrContentLength
	}
	w.written += int64(len(p))
	if w.head {
		return len(p), nil
	}
	if !w.sentHeader {
		if len(w.buf)+len(p) <= responseBufferSize {
			w.buf = append(w.buf, p...)
			return len(p), nil
		}
		if err := w.sendHeader(w.declared); err != nil {
			return 0, err
		}
	}
	n, err := w.sink.Write(p)
	if err != nil {
		w.err = err
	}
	return n, err
}

func (w *response) WriteString(s string) (int, error) {
	return w.Write(bytesconv.S2B(s))
}

func (w *response) Flush() error {
	w.live()
	if w.hijacked {
		return ErrHijacked
	}
	if !w.wroteHeader {
		w.WriteHeader(200)
	}
	if w.err != nil {
		return w.err
	}
	if !w.sentHeader {
		if err := w.sendHeader(w.declared); err != nil {
			return err
		}
	}
	if err := w.sink.Flush(); err != nil {
		w.err = err
		return err
	}
	return nil
}

// Trailer returns the trailer fields to send after the body. See [TrailerSetter].
func (w *response) Trailer() *Header {
	w.live()
	return &w.trailer
}

// Hijack hands the connection to the caller. The ResponseWriter must not be used afterwards.
func (w *response) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	w.live()
	if w.hijacked {
		return nil, nil, ErrHijacked
	}
	h, ok := w.sink.(exchange.Hijacker)
	if !ok {
		return nil, nil, ErrNotSupported
	}
	conn, rw, err := h.Hijack()
	if err == nil {
		w.hijacked = true
	}
	return conn, rw, err
}

// finish completes the response after the handler returns.
func (w *response) finish() error {
	if w.hijacked {
		return nil
	}
	if !w.wroteHeader {
		w.WriteHeader(200)
	}
	if w.err != nil {
		return w.err
	}
	short := w.declared >= 0 && w.written < w.declared && !w.head && !w.noBody
	hasTrailer := w.trailer.Len() > 0
	if !w.sentHeader {
		cl := w.declared
		if cl < 0 && !w.noBody {
			cl = w.written
		}
		if cs, ok := w.sink.(exchange.CompleteSink); ok && !short {
			return w.sendComplete(cs, cl)
		}
		// HTTP/1.1 can carry trailers only in a chunked body.
		if hasTrailer && w.declared < 0 {
			cl = -1
		}
		if err := w.sendHeader(cl); err != nil {
			return err
		}
	}
	if short {
		w.err = ErrContentLength
		return w.err
	}
	var err error
	if ts, ok := w.sink.(exchange.TrailerSink); ok && hasTrailer {
		err = ts.FinishTrailer((*headerList)(&w.trailer))
	} else {
		err = w.sink.Finish()
	}
	if err != nil {
		w.err = err
	}
	return err
}

// sendComplete hands a fully buffered response to the protocol in one call.
func (w *response) sendComplete(cs exchange.CompleteSink, contentLength int64) error {
	w.sentHeader = true
	w.setContentLength(contentLength)
	var trailer exchange.HeaderList
	if w.trailer.Len() > 0 {
		trailer = (*headerList)(&w.trailer)
	}
	err := cs.WriteComplete(w.status, (*headerList)(&w.header), contentLength, w.buf, trailer)
	w.buf = w.buf[:0]
	if err != nil {
		w.err = err
	}
	return err
}

// setContentLength rewrites Content-Length to match the framing the protocol will use, since the header
// is sent lazily.
func (w *response) setContentLength(contentLength int64) {
	if w.noBody {
		return
	}
	if contentLength >= 0 {
		var b [20]byte
		w.header.SetBytes("content-length", strconv.AppendInt(b[:0], contentLength, 10))
	} else {
		w.header.Del("content-length")
	}
}

func (w *response) sendHeader(contentLength int64) error {
	w.sentHeader = true
	w.setContentLength(contentLength)
	if err := w.sink.WriteHeader(w.status, (*headerList)(&w.header), contentLength); err != nil {
		w.err = err
		return err
	}
	if len(w.buf) > 0 {
		_, err := w.sink.Write(w.buf)
		w.buf = w.buf[:0]
		if err != nil {
			w.err = err
			return err
		}
	}
	return nil
}

func (w *response) live() {
	if w.dead {
		panic("qilat: ResponseWriter used after ServeHTTP returned")
	}
}

func (w *response) reset() {
	buf := w.buf[:0]
	w.header.Reset()
	w.trailer.Reset()
	header, trailer := w.header, w.trailer
	*w = response{header: header, trailer: trailer, buf: buf}
}

func (w *response) poison() {
	w.dead = true
	w.header.dead = true
	w.trailer.dead = true
}
