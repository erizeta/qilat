package qilat

import (
	"errors"
	"fmt"
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

var (
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
	status      int
	wroteHeader bool
	sentHeader  bool
	noBody      bool // the status code forbids a body
	head        bool // HEAD request: count body bytes for Content-Length but send none
	declared    int64
	written     int64
	buf         []byte
	err         error
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
	// Informational responses need protocol support and will be sent once protocols provide it.
	if code < 200 && code != 101 {
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

// finish completes the response after the handler returns.
func (w *response) finish() error {
	if !w.wroteHeader {
		w.WriteHeader(200)
	}
	if w.err != nil {
		return w.err
	}
	if !w.sentHeader {
		cl := w.declared
		if cl < 0 && !w.noBody {
			cl = w.written
		}
		if err := w.sendHeader(cl); err != nil {
			return err
		}
	}
	if w.declared >= 0 && w.written < w.declared && !w.head && !w.noBody {
		w.err = ErrContentLength
		return w.err
	}
	if err := w.sink.Finish(); err != nil {
		w.err = err
		return err
	}
	return nil
}

func (w *response) sendHeader(contentLength int64) error {
	w.sentHeader = true
	// The header is sent lazily, so rewrite Content-Length to match the framing the protocol will use.
	if !w.noBody {
		if contentLength >= 0 {
			var b [20]byte
			w.header.SetBytes("content-length", strconv.AppendInt(b[:0], contentLength, 10))
		} else {
			w.header.Del("content-length")
		}
	}
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
	header := w.header
	*w = response{header: header, buf: buf}
}

func (w *response) poison() {
	w.dead = true
	w.header.dead = true
}
