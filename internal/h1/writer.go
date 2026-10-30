package h1

import (
	"bufio"
	"io"
	"net"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/erizeta/qilat/internal/exchange"
)

// sink writes one response to the connection. It lives inside Conn so passing it to the core does not allocate.
type sink struct {
	c *Conn
}

var (
	_ exchange.ResponseSink      = (*sink)(nil)
	_ exchange.InformationalSink = (*sink)(nil)
	_ exchange.Hijacker          = (*sink)(nil)
	_ exchange.TrailerSink       = (*sink)(nil)
	_ exchange.ReaderFromSink    = (*sink)(nil)
)

func (s *sink) WriteHeader(status int, h exchange.HeaderList, contentLength int64) error {
	c := s.c
	bw := c.writer()
	c.headerWritten = true
	keepAlive := c.info.keepAlive && !c.cfg.ShuttingDown.Load()

	writeStatusLine(bw, status)
	hasDate := false
	for i := range h.Len() {
		name, value := h.At(i)
		switch string(name) {
		case "connection":
			if hasToken(value, "close") {
				keepAlive = false
			}
			continue
		case "transfer-encoding":
			// Framing is decided here from contentLength; a handler-set value would contradict it.
			continue
		case "date":
			hasDate = true
		}
		writeField(bw, name, value)
	}
	if !hasDate {
		_, _ = bw.Write(dateLine())
	}
	c.chunkedResp = false
	if contentLength < 0 && bodyAllowed(status, c.info.head) {
		if c.info.minor == 1 {
			_, _ = bw.WriteString("Transfer-Encoding: chunked\r\n")
			c.chunkedResp = true
		} else {
			keepAlive = false // HTTP/1.0 has no chunked coding: the body ends when the connection closes
		}
	}
	switch {
	case !keepAlive:
		_, err := bw.WriteString("Connection: close\r\n\r\n")
		c.respKeepAlive = false
		return err
	case c.info.minor == 0:
		_, _ = bw.WriteString("Connection: keep-alive\r\n")
	}
	c.respKeepAlive = true
	_, err := bw.WriteString("\r\n")
	return err
}

func (s *sink) Write(p []byte) (int, error) {
	c := s.c
	if !c.chunkedResp {
		return c.bw.Write(p)
	}
	if len(p) == 0 {
		return 0, nil
	}
	hdr := strconv.AppendInt(c.scratch[:0], int64(len(p)), 16)
	hdr = append(hdr, '\r', '\n')
	if _, err := c.bw.Write(hdr); err != nil {
		return 0, err
	}
	n, err := c.bw.Write(p)
	if err != nil {
		return n, err
	}
	_, err = c.bw.WriteString("\r\n")
	return n, err
}

func (s *sink) Flush() error { return s.c.bw.Flush() }

// ReadFrom copies a body of known length. Once its buffer is flushed, bufio hands the copy to the
// connection's ReadFrom, which is sendfile for a file on a plain TCP connection.
func (s *sink) ReadFrom(r io.Reader) (int64, error) { return s.c.bw.ReadFrom(r) }

func (s *sink) Finish() error {
	c := s.c
	if c.chunkedResp {
		if _, err := c.bw.WriteString("0\r\n\r\n"); err != nil {
			return err
		}
	}
	return s.flushAfterResponse()
}

// FinishTrailer ends a chunked body with trailer fields. Other bodies cannot carry trailers, so they are dropped.
func (s *sink) FinishTrailer(trailer exchange.HeaderList) error {
	c := s.c
	if !c.chunkedResp {
		return s.Finish()
	}
	_, _ = c.bw.WriteString("0\r\n")
	for i := range trailer.Len() {
		name, value := trailer.At(i)
		switch string(name) {
		case "connection", "transfer-encoding", "content-length", "host", "te", "trailer", "keep-alive", "upgrade":
			continue
		}
		writeField(c.bw, name, value)
	}
	if _, err := c.bw.WriteString("\r\n"); err != nil {
		return err
	}
	return s.flushAfterResponse()
}

func (s *sink) flushAfterResponse() error {
	c := s.c
	// With pipelined requests already buffered, keep batching responses; the conn loop flushes before blocking.
	if c.respKeepAlive && c.br.Buffered() > 0 {
		return nil
	}
	return c.bw.Flush()
}

func (s *sink) WriteInformational(status int, h exchange.HeaderList) error {
	c := s.c
	// HTTP/1.0 clients do not understand 1xx responses.
	if c.info.minor == 0 || c.headerWritten {
		return nil
	}
	if status == 100 {
		if c.continueSent {
			return nil
		}
		c.continueSent = true
	}
	bw := c.writer()
	writeStatusLine(bw, status)
	for i := range h.Len() {
		name, value := h.At(i)
		switch string(name) {
		case "connection", "transfer-encoding", "content-length":
			continue
		}
		writeField(bw, name, value)
	}
	if _, err := bw.WriteString("\r\n"); err != nil {
		return err
	}
	return bw.Flush()
}

func (s *sink) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	c := s.c
	c.cr.abortPendingRead()
	bw := c.writer()
	if err := bw.Flush(); err != nil {
		return nil, nil, err
	}
	c.watchMu.Lock()
	c.hijacked = true
	c.watchMu.Unlock()
	c.state.Store(stateHijacked)
	if err := c.nc.SetDeadline(time.Time{}); err != nil {
		return nil, nil, err
	}
	return c.nc, bufio.NewReadWriter(c.br, bw), nil
}

func bodyAllowed(status int, head bool) bool {
	return !head && status >= 200 && status != 204 && status != 304
}

// writeField writes name in canonical form (Content-Type), which old HTTP/1.1 clients expect.
func writeField(bw *bufio.Writer, name, value []byte) {
	upper := true
	for _, ch := range name {
		if upper && 'a' <= ch && ch <= 'z' {
			ch -= 'a' - 'A'
		}
		_ = bw.WriteByte(ch)
		upper = ch == '-'
	}
	_, _ = bw.WriteString(": ")
	_, _ = bw.Write(value)
	_, _ = bw.WriteString("\r\n")
}

func writeStatusLine(bw *bufio.Writer, status int) {
	if status >= 100 && status < len(statusLines) && statusLines[status] != "" {
		_, _ = bw.WriteString(statusLines[status])
		return
	}
	var b [16]byte
	_, _ = bw.WriteString("HTTP/1.1 ")
	_, _ = bw.Write(strconv.AppendInt(b[:0], int64(status), 10))
	_, _ = bw.WriteString(" \r\n")
}

// statusLines holds preformatted status lines for registered codes.
var statusLines = func() (lines [600]string) {
	for code, text := range statusText {
		lines[code] = "HTTP/1.1 " + strconv.Itoa(code) + " " + text + "\r\n"
	}
	return lines
}()

var statusText = map[int]string{
	100: "Continue", 101: "Switching Protocols", 102: "Processing", 103: "Early Hints",
	200: "OK", 201: "Created", 202: "Accepted", 203: "Non-Authoritative Information", 204: "No Content",
	205: "Reset Content", 206: "Partial Content", 207: "Multi-Status", 208: "Already Reported", 226: "IM Used",
	300: "Multiple Choices", 301: "Moved Permanently", 302: "Found", 303: "See Other", 304: "Not Modified",
	305: "Use Proxy", 307: "Temporary Redirect", 308: "Permanent Redirect",
	400: "Bad Request", 401: "Unauthorized", 402: "Payment Required", 403: "Forbidden", 404: "Not Found",
	405: "Method Not Allowed", 406: "Not Acceptable", 407: "Proxy Authentication Required", 408: "Request Timeout",
	409: "Conflict", 410: "Gone", 411: "Length Required", 412: "Precondition Failed", 413: "Content Too Large",
	414: "URI Too Long", 415: "Unsupported Media Type", 416: "Range Not Satisfiable", 417: "Expectation Failed",
	418: "I'm a teapot", 421: "Misdirected Request", 422: "Unprocessable Content", 423: "Locked",
	424: "Failed Dependency", 425: "Too Early", 426: "Upgrade Required", 428: "Precondition Required",
	429: "Too Many Requests", 431: "Request Header Fields Too Large", 451: "Unavailable For Legal Reasons",
	500: "Internal Server Error", 501: "Not Implemented", 502: "Bad Gateway", 503: "Service Unavailable",
	504: "Gateway Timeout", 505: "HTTP Version Not Supported", 506: "Variant Also Negotiates",
	507: "Insufficient Storage", 508: "Loop Detected", 510: "Not Extended", 511: "Network Authentication Required",
}

// hasToken reports whether the comma-separated list v contains token, ignoring case.
func hasToken(v []byte, token string) bool {
	for len(v) > 0 {
		var opt []byte
		opt, v, _ = cutByte(v, ',')
		if equalFold(trimOWS(opt), token) {
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

type dateValue struct {
	sec  int64
	line []byte
}

var dateCache atomic.Pointer[dateValue]

// dateLine returns the "Date: ...\r\n" line for the current second, formatting it at most once per second.
func dateLine() []byte {
	now := time.Now()
	sec := now.Unix()
	if d := dateCache.Load(); d != nil && d.sec == sec {
		return d.line
	}
	line := now.UTC().AppendFormat([]byte("Date: "), "Mon, 02 Jan 2006 15:04:05 GMT")
	line = append(line, "\r\n"...)
	dateCache.Store(&dateValue{sec: sec, line: line})
	return line
}
