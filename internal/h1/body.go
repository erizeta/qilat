package h1

import (
	"bytes"
	"errors"
	"io"

	"github.com/erizeta/qilat/internal/bytesconv"
	"github.com/erizeta/qilat/internal/exchange"
)

type bodyMode uint8

const (
	bodyNone bodyMode = iota
	bodyLength
	bodyChunked
)

type chunkState uint8

const (
	chunkSize    chunkState = iota // expecting a chunk-size line
	chunkData                      // inside chunk data
	chunkDataEnd                   // expecting the CRLF after chunk data
	chunkDone                      // last chunk and trailers consumed
)

const (
	// maxChunkLine bounds a chunk-size line including extensions, which are otherwise unbounded input.
	maxChunkLine = 4 << 10
	// maxChunkSizeDigits keeps chunk sizes within int64.
	maxChunkSizeDigits = 15
)

var (
	errChunkSize  = errors.New("h1: invalid chunk size line")
	errChunkCRLF  = errors.New("h1: chunk data not followed by CRLF")
	errTrailer    = errors.New("h1: invalid trailer field")
	errBodyClosed = errors.New("h1: request body read after the handler returned")
)

// body reads a request body framed by Content-Length or chunked transfer coding.
type body struct {
	c         *Conn
	mode      bodyMode
	remaining int64 // bytes left in the body (length) or in the current chunk (chunked)
	limit     int64 // maximum total body bytes; negative means unlimited
	total     int64
	state     chunkState
	err       error // sticky
	eof       bool
}

func (b *body) reset(c *Conn, info *reqInfo, limit int64) {
	*b = body{c: c, limit: limit}
	switch {
	case info.chunked:
		b.mode = bodyChunked
	case info.contentLength > 0:
		b.mode = bodyLength
		b.remaining = info.contentLength
	}
	b.eof = b.mode == bodyNone
}

func (b *body) Read(p []byte) (int, error) {
	if b.err != nil {
		return 0, b.err
	}
	if b.eof {
		return 0, io.EOF
	}
	if len(p) == 0 {
		return 0, nil
	}
	b.c.beforeBodyRead()
	var n int
	var err error
	if b.mode == bodyLength {
		n, err = b.readLength(p)
	} else {
		n, err = b.readChunked(p)
	}
	if err != nil && !errors.Is(err, io.EOF) {
		b.err = err
	}
	return n, err
}

func (b *body) readLength(p []byte) (int, error) {
	if int64(len(p)) > b.remaining {
		p = p[:b.remaining]
	}
	n, err := b.c.br.Read(p)
	b.remaining -= int64(n)
	if b.remaining == 0 {
		b.finish()
		return n, io.EOF
	}
	if err != nil {
		if errors.Is(err, io.EOF) {
			err = io.ErrUnexpectedEOF
		}
		return n, err
	}
	return n, nil
}

func (b *body) readChunked(p []byte) (int, error) {
	for {
		switch b.state {
		case chunkSize:
			size, err := b.readChunkSize()
			if err != nil {
				return 0, err
			}
			if size == 0 {
				if err := b.readTrailers(); err != nil {
					return 0, err
				}
				b.finish()
				return 0, io.EOF
			}
			if b.limit >= 0 && b.total+size > b.limit {
				return 0, exchange.ErrBodyTooLarge
			}
			b.remaining = size
			b.state = chunkData
		case chunkData:
			if int64(len(p)) > b.remaining {
				p = p[:b.remaining]
			}
			n, err := b.c.br.Read(p)
			b.remaining -= int64(n)
			b.total += int64(n)
			if b.remaining == 0 {
				b.state = chunkDataEnd
			}
			if err != nil {
				if errors.Is(err, io.EOF) {
					err = io.ErrUnexpectedEOF
				}
				return n, err
			}
			// Return data now; reading the trailing CRLF could block on a client that sends it later.
			return n, nil
		case chunkDataEnd:
			if err := b.readCRLF(); err != nil {
				return 0, err
			}
			b.state = chunkSize
		case chunkDone:
			return 0, io.EOF
		}
	}
}

func (b *body) readChunkSize() (int64, error) {
	lr := &b.c.lr
	lr.budget = maxChunkLine
	line, err := lr.readLine()
	if err != nil {
		return 0, chunkReadError(err, errChunkSize)
	}
	return parseChunkSize(line)
}

func parseChunkSize(line []byte) (int64, error) {
	var n int64
	i := 0
	for ; i < len(line); i++ {
		d, ok := bytesconv.Unhex(line[i])
		if !ok {
			break
		}
		if i == maxChunkSizeDigits {
			return 0, errChunkSize
		}
		n = n<<4 | int64(d)
	}
	if i == 0 {
		return 0, errChunkSize
	}
	ext := line[i:]
	for len(ext) > 0 && (ext[0] == ' ' || ext[0] == '\t') {
		ext = ext[1:]
	}
	if len(ext) > 0 && (ext[0] != ';' || !validFieldValue(ext)) {
		return 0, errChunkSize
	}
	return n, nil
}

func (b *body) readCRLF() error {
	var crlf [2]byte
	if _, err := io.ReadFull(b.c.br, crlf[:]); err != nil {
		return chunkReadError(err, errChunkCRLF)
	}
	if crlf != [2]byte{'\r', '\n'} {
		return errChunkCRLF
	}
	return nil
}

// readTrailers validates and discards the trailer section.
func (b *body) readTrailers() error {
	lr := &b.c.lr
	lr.budget = b.c.cfg.MaxHeaderBytes
	for {
		line, err := lr.readLine()
		if err != nil {
			return chunkReadError(err, errTrailer)
		}
		if len(line) == 0 {
			return nil
		}
		colon := bytes.IndexByte(line, ':')
		if colon <= 0 || line[0] == ' ' || line[0] == '\t' || !validToken(line[:colon]) ||
			!validFieldValue(line[colon+1:]) {
			return errTrailer
		}
	}
}

// chunkReadError maps line-level failures inside a body to malformed, and EOF to io.ErrUnexpectedEOF.
func chunkReadError(err, malformed error) error {
	var pe *parseError
	switch {
	case errors.Is(err, io.EOF):
		return io.ErrUnexpectedEOF
	case errors.As(err, &pe):
		return malformed
	}
	return err
}

func (b *body) finish() {
	b.eof = true
	b.state = chunkDone
	b.c.onBodyEOF()
}

// drain discards the unread body, reading at most limit bytes. It reports whether the body was fully consumed.
func (b *body) drain(limit int64) bool {
	if b.err != nil {
		return false
	}
	if b.eof {
		return true
	}
	if b.mode == bodyLength && b.remaining > limit {
		return false
	}
	var buf [512]byte
	var read int64
	for read <= limit {
		n, err := b.Read(buf[:])
		read += int64(n)
		if errors.Is(err, io.EOF) {
			return true
		}
		if err != nil {
			return false
		}
	}
	return false
}

// close makes later reads fail, for handlers that keep the body past ServeHTTP.
func (b *body) close() {
	if b.err == nil {
		b.err = errBodyClosed
	}
}

func validToken(b []byte) bool {
	for _, c := range b {
		if !isTokenChar(c) {
			return false
		}
	}
	return len(b) > 0
}

func isTokenChar(c byte) bool {
	switch {
	case 'a' <= c && c <= 'z', 'A' <= c && c <= 'Z', '0' <= c && c <= '9':
		return true
	}
	return bytes.IndexByte([]byte("!#$%&'*+-.^_`|~"), c) >= 0
}
