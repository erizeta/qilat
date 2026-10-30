package h1

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/erizeta/qilat/internal/bytesconv"
	"github.com/erizeta/qilat/internal/exchange"
)

var (
	errStatusLine       = errors.New("h1: malformed HTTP response status line")
	errResponseHeader   = errors.New("h1: malformed HTTP response header")
	errResponseLength   = errors.New("h1: invalid Content-Length in HTTP response")
	errResponseTooLarge = errors.New("h1: HTTP response header too large")
	errConnBusy         = errors.New("h1: connection is busy")
	errRespBodyClosed   = errors.New("h1: response body read after close")
)

// ClientConfig holds the client settings for HTTP/1.1 connections, with defaults already resolved.
type ClientConfig struct {
	MaxHeaderBytes        int
	ResponseHeaderTimeout time.Duration // zero means no limit
}

// ClientConn sends requests on one HTTP/1.1 connection, one at a time. Once a response body is done, it
// calls done with whether the connection can carry another request; done must close it otherwise.
type ClientConn struct {
	nc   net.Conn
	cfg  *ClientConfig
	br   *bufio.Reader
	bw   *bufio.Writer
	lr   lineReader
	body clientBody
	done func(c *ClientConn, reusable bool)

	used     bool // a request was sent before, so a failure may mean the server closed an idle connection
	busy     bool
	broken   bool // writing the request failed, so the connection's state is unknown
	ctx      context.Context
	stop     func() bool
	onCancel func() // built once, so watching a context allocates only when it can be canceled
	canceled atomic.Bool
	scratch  [24]byte
	copyBuf  []byte // for request bodies, allocated on first use
}

// NewClientConn wraps an established connection.
func NewClientConn(nc net.Conn, cfg *ClientConfig, done func(c *ClientConn, reusable bool)) *ClientConn {
	c := &ClientConn{nc: nc, cfg: cfg, done: done}
	c.br = bufio.NewReaderSize(nc, bufferSize)
	c.bw = bufio.NewWriterSize(nc, bufferSize)
	c.lr.br = c.br
	c.body.c = c
	c.onCancel = func() {
		c.canceled.Store(true)
		_ = c.nc.SetDeadline(aLongTimeAgo)
	}
	return c
}

// Close closes the underlying connection.
func (c *ClientConn) Close() error { return c.nc.Close() }

// Reused reports whether the connection has carried a request before.
func (c *ClientConn) Reused() bool { return c.used }

// RoundTrip sends req and reads the response header into resp. The body is read through resp afterwards.
func (c *ClientConn) RoundTrip(ctx context.Context, req *exchange.ClientRequest, resp exchange.ClientResponse) error {
	if c.busy {
		return errConnBusy
	}
	c.busy = true
	c.broken = false
	reused := c.used
	c.used = true
	c.ctx = ctx
	c.canceled.Store(false)
	if ctx.Done() != nil {
		c.stop = context.AfterFunc(ctx, c.onCancel)
	}

	if err := c.writeRequest(req); err != nil {
		// The response may already be on its way, as when a server rejects a large upload early.
		c.broken = true
		if rerr := c.readResponse(req, resp, false); rerr == nil {
			return nil
		}
		return c.fail(err, reused)
	}
	if err := c.readResponse(req, resp, reused); err != nil {
		return c.fail(err, false)
	}
	return nil
}

// fail ends a round trip that produced no response. notSent marks errors that may be retried.
func (c *ClientConn) fail(err error, notSent bool) error {
	if c.canceled.Load() {
		err = context.Cause(c.ctx)
	} else if notSent {
		err = exchange.NotProcessed(err)
	}
	c.finish(false)
	return err
}

// finish ends the round trip once the response body is done, and hands the connection back.
func (c *ClientConn) finish(reusable bool) {
	if c.stop != nil {
		c.stop()
		c.stop = nil
	}
	if c.canceled.Load() || c.broken {
		reusable = false
	}
	c.ctx = nil
	c.busy = false
	c.done(c, reusable)
}

func (c *ClientConn) writeRequest(req *exchange.ClientRequest) error {
	bw := c.bw
	_, _ = bw.WriteString(req.Method)
	_ = bw.WriteByte(' ')
	_, _ = bw.Write(req.Target)
	_, _ = bw.WriteString(" HTTP/1.1\r\nHost: ")
	_, _ = bw.Write(req.Authority)
	_, _ = bw.WriteString("\r\n")
	for i := range req.Header.Len() {
		name, value := req.Header.At(i)
		switch string(name) {
		case "host", "content-length", "transfer-encoding", "proxy-authorization":
			continue // decided here from the request itself
		}
		writeField(bw, name, value)
	}
	if len(req.ProxyAuthorization) > 0 {
		_, _ = bw.WriteString("Proxy-Authorization: ")
		_, _ = bw.Write(req.ProxyAuthorization)
		_, _ = bw.WriteString("\r\n")
	}
	chunked := false
	switch {
	case req.Body != nil && req.ContentLength >= 0:
		writeContentLength(bw, req.ContentLength, &c.scratch)
	case req.Body != nil:
		_, _ = bw.WriteString("Transfer-Encoding: chunked\r\n")
		chunked = true
	case req.Method == "POST" || req.Method == "PUT" || req.Method == "PATCH":
		// RFC 9110 section 8.6: a body-carrying method without a body says so.
		_, _ = bw.WriteString("Content-Length: 0\r\n")
	}
	if _, err := bw.WriteString("\r\n"); err != nil {
		return err
	}
	if req.Body != nil {
		if err := c.writeBody(req.Body, req.ContentLength, chunked); err != nil {
			return err
		}
	}
	return bw.Flush()
}

func writeContentLength(bw *bufio.Writer, n int64, scratch *[24]byte) {
	_, _ = bw.WriteString("Content-Length: ")
	_, _ = bw.Write(strconv.AppendInt(scratch[:0], n, 10))
	_, _ = bw.WriteString("\r\n")
}

// sizedReader is implemented by *bytes.Reader and *strings.Reader, whose WriteTo sends the rest in one write.
type sizedReader interface {
	io.WriterTo
	Len() int
}

func (c *ClientConn) writeBody(body io.Reader, length int64, chunked bool) error {
	if c.copyBuf == nil {
		c.copyBuf = make([]byte, bufferSize)
	}
	buf := c.copyBuf
	if !chunked {
		if sr, ok := body.(sizedReader); ok && int64(sr.Len()) == length {
			_, err := sr.WriteTo(c.bw)
			return err
		}
		// Not io.Copy: bufio would hand the copy to the socket's ReadFrom, which allocates a buffer each time.
		for length > 0 {
			n, err := body.Read(buf[:min(int64(len(buf)), length)])
			length -= int64(n)
			if _, werr := c.bw.Write(buf[:n]); werr != nil {
				return werr
			}
			if errors.Is(err, io.EOF) && length > 0 {
				return io.ErrUnexpectedEOF
			}
			if err != nil && !errors.Is(err, io.EOF) {
				return err
			}
		}
		return nil
	}
	for {
		n, err := body.Read(buf)
		if n > 0 {
			hdr := strconv.AppendInt(c.scratch[:0], int64(n), 16)
			_, _ = c.bw.Write(append(hdr, '\r', '\n'))
			_, _ = c.bw.Write(buf[:n])
			if _, werr := c.bw.WriteString("\r\n"); werr != nil {
				return werr
			}
		}
		if errors.Is(err, io.EOF) {
			_, err = c.bw.WriteString("0\r\n\r\n")
			return err
		}
		if err != nil {
			return err
		}
	}
}

// readResponse reads the final response header, skipping 1xx responses. firstByteRetryable marks an EOF
// before any byte as a request the server never processed.
func (c *ClientConn) readResponse(req *exchange.ClientRequest, resp exchange.ClientResponse, firstByteRetryable bool) error {
	if d := c.cfg.ResponseHeaderTimeout; d > 0 {
		_ = c.nc.SetReadDeadline(time.Now().Add(d))
	}
	if firstByteRetryable {
		if _, err := c.br.Peek(1); err != nil {
			if errors.Is(err, io.EOF) || isConnReset(err) {
				return exchange.NotProcessed(err)
			}
			return err
		}
	}
	for {
		status, info, err := c.readHeader(resp)
		if err != nil {
			return err
		}
		if status >= 100 && status < 200 && status != 101 {
			continue
		}
		if c.cfg.ResponseHeaderTimeout > 0 && !c.canceled.Load() {
			_ = c.nc.SetReadDeadline(time.Time{})
		}
		resp.SetStatus(status, 1, info.minor)
		noBody := req.Method == "HEAD" || status == 204 || status == 304 || status == 101
		c.body.reset(resp, info, noBody)
		if c.body.mode == clientBodyNone {
			resp.SetBody(nil, 0)
			c.finish(c.body.keepAlive)
			return nil
		}
		cl := int64(-1)
		if c.body.mode == clientBodyLength {
			cl = c.body.remaining
		}
		resp.SetBody(&c.body, cl)
		return nil
	}
}

// respInfo is the framing of a parsed response.
type respInfo struct {
	minor         int
	contentLength int64
	chunked       bool
	keepAlive     bool
}

func (c *ClientConn) readHeader(resp exchange.ClientResponse) (int, respInfo, error) {
	info := respInfo{contentLength: -1}
	c.lr.budget = c.cfg.MaxHeaderBytes
	line, err := c.lr.readLine()
	if err != nil {
		return 0, info, clientLineError(err)
	}
	status, minor, ok := parseStatusLine(line)
	if !ok {
		return 0, info, errStatusLine
	}
	info.minor = minor
	info.keepAlive = minor == 1
	for {
		line, err := c.lr.readLine()
		if err != nil {
			return 0, info, clientLineError(err)
		}
		if len(line) == 0 {
			break
		}
		colon := bytes.IndexByte(line, ':')
		if colon <= 0 || !validToken(line[:colon]) || line[0] == ' ' || line[0] == '\t' {
			return 0, info, errResponseHeader
		}
		name, value := line[:colon], trimOWS(line[colon+1:])
		if !validFieldValue(value) {
			return 0, info, errResponseHeader
		}
		switch {
		case equalFold(name, "content-length"):
			n, ok := bytesconv.ParseUint(value)
			if !ok || info.contentLength >= 0 && info.contentLength != n {
				return 0, info, errResponseLength
			}
			info.contentLength = n
		case equalFold(name, "transfer-encoding"):
			info.chunked = hasToken(value, "chunked")
		case equalFold(name, "connection"):
			if hasToken(value, "close") {
				info.keepAlive = false
			} else if minor == 0 && hasToken(value, "keep-alive") {
				info.keepAlive = true
			}
		}
		if status >= 200 || status == 101 {
			if err := resp.AddHeader(name, value); err != nil {
				return 0, info, err
			}
		}
	}
	return status, info, nil
}

func clientLineError(err error) error {
	switch {
	case errors.Is(err, errHeaderTooLarge):
		return errResponseTooLarge
	case errors.Is(err, errBareLF):
		return errResponseHeader
	}
	return err
}

// parseStatusLine parses "HTTP/1.x 200 reason".
func parseStatusLine(line []byte) (status, minor int, ok bool) {
	if len(line) < 12 || string(line[:7]) != "HTTP/1." || line[8] != ' ' || !isDigit(line[7]) {
		return 0, 0, false
	}
	minor = int(line[7] - '0')
	code := line[9:12]
	if !isDigit(code[0]) || !isDigit(code[1]) || !isDigit(code[2]) || len(line) > 12 && line[12] != ' ' {
		return 0, 0, false
	}
	status = int(code[0]-'0')*100 + int(code[1]-'0')*10 + int(code[2]-'0')
	return status, minor, status >= 100
}

func isConnReset(err error) bool {
	var ne *net.OpError
	return errors.As(err, &ne) && !ne.Timeout()
}

type clientBodyMode uint8

const (
	clientBodyNone clientBodyMode = iota
	clientBodyLength
	clientBodyChunked
	clientBodyClose // read until the server closes the connection
)

// clientBody reads a response body and returns the connection to its pool at EOF.
type clientBody struct {
	c         *ClientConn
	resp      exchange.ClientResponse
	mode      clientBodyMode
	remaining int64
	state     chunkState
	keepAlive bool
	err       error
	eof       bool
}

func (b *clientBody) reset(resp exchange.ClientResponse, info respInfo, noBody bool) {
	*b = clientBody{c: b.c, resp: resp, keepAlive: info.keepAlive}
	switch {
	case noBody:
	case info.chunked:
		b.mode = clientBodyChunked
	case info.contentLength > 0:
		b.mode = clientBodyLength
		b.remaining = info.contentLength
	case info.contentLength == 0:
	default:
		b.mode = clientBodyClose
		b.keepAlive = false
	}
}

func (b *clientBody) Read(p []byte) (int, error) {
	if b.err != nil {
		return 0, b.err
	}
	if b.eof {
		return 0, io.EOF
	}
	if len(p) == 0 {
		return 0, nil
	}
	var n int
	var err error
	switch b.mode {
	case clientBodyLength:
		n, err = b.c.br.Read(p[:min(int64(len(p)), b.remaining)])
		b.remaining -= int64(n)
		if b.remaining == 0 {
			err = io.EOF
		} else if errors.Is(err, io.EOF) {
			err = io.ErrUnexpectedEOF
		}
	case clientBodyChunked:
		n, err = b.readChunked(p)
	case clientBodyClose:
		n, err = b.c.br.Read(p)
	}
	if errors.Is(err, io.EOF) {
		b.eof = true
		b.c.finish(b.keepAlive)
		return n, io.EOF
	}
	if err != nil {
		if b.c.canceled.Load() {
			err = context.Cause(b.c.ctx)
		}
		b.err = err
		b.c.finish(false)
	}
	return n, err
}

func (b *clientBody) readChunked(p []byte) (int, error) {
	for {
		switch b.state {
		case chunkSize:
			b.c.lr.budget = maxChunkLine
			line, err := b.c.lr.readLine()
			if err != nil {
				return 0, chunkReadError(err, errChunkSize)
			}
			n, err := parseChunkSize(line)
			if err != nil {
				return 0, err
			}
			if n == 0 {
				return 0, b.readTrailers()
			}
			b.remaining = n
			b.state = chunkData
		case chunkData:
			n, err := b.c.br.Read(p[:min(int64(len(p)), b.remaining)])
			b.remaining -= int64(n)
			if b.remaining == 0 {
				b.state = chunkDataEnd
			}
			if errors.Is(err, io.EOF) {
				err = io.ErrUnexpectedEOF
			}
			return n, err
		case chunkDataEnd:
			var crlf [2]byte
			if _, err := io.ReadFull(b.c.br, crlf[:]); err != nil {
				return 0, chunkReadError(err, errChunkCRLF)
			}
			if crlf != [2]byte{'\r', '\n'} {
				return 0, errChunkCRLF
			}
			b.state = chunkSize
		}
	}
}

// readTrailers hands the trailer section to the response and reports io.EOF at its end.
func (b *clientBody) readTrailers() error {
	b.c.lr.budget = b.c.cfg.MaxHeaderBytes
	for {
		line, err := b.c.lr.readLine()
		if err != nil {
			return chunkReadError(err, errTrailer)
		}
		if len(line) == 0 {
			b.state = chunkDone
			return io.EOF
		}
		colon := bytes.IndexByte(line, ':')
		if colon <= 0 || line[0] == ' ' || line[0] == '\t' || !validToken(line[:colon]) {
			return errTrailer
		}
		value := trimOWS(line[colon+1:])
		if !validFieldValue(value) || b.resp.AddTrailer(line[:colon], value) != nil {
			return errTrailer
		}
	}
}

// Close abandons the rest of the body. The connection is reused only when the rest is already buffered.
func (b *clientBody) Close() error {
	if b.eof || b.err != nil {
		return nil
	}
	reusable := b.keepAlive && b.mode == clientBodyLength && b.remaining <= int64(b.c.br.Buffered())
	if reusable {
		_, _ = b.c.br.Discard(int(b.remaining))
	}
	b.err = errRespBodyClosed
	b.c.finish(reusable)
	return nil
}
