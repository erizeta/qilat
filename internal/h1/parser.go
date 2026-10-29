// Package h1 implements the HTTP/1.1 server protocol (RFC 9112) on top of the exchange contract.
package h1

import (
	"bufio"
	"bytes"
	"errors"
	"io"

	"github.com/erizeta/qilat/internal/bytesconv"
	"github.com/erizeta/qilat/internal/exchange"
)

// parseError is a request the server must reject with status and then close the connection.
type parseError struct {
	status int
	msg    string
}

func (e *parseError) Error() string { return "h1: " + e.msg }

// Errors are preallocated because malformed traffic is attacker-controlled and must stay cheap.
var (
	errMalformedRequestLine = &parseError{400, "malformed request line"}
	errMalformedHeader      = &parseError{400, "malformed header field"}
	errInvalidMethod        = &parseError{400, "invalid method"}
	errInvalidTarget        = &parseError{400, "invalid request target"}
	errInvalidHost          = &parseError{400, "invalid Host header"}
	errMissingHost          = &parseError{400, "missing Host header"}
	errBareLF               = &parseError{400, "line not terminated by CRLF"}
	errObsFold              = &parseError{400, "obsolete line folding"}
	errDuplicateHost        = &parseError{400, "duplicate Host header"}
	errDuplicateLength      = &parseError{400, "duplicate Content-Length"}
	errInvalidLength        = &parseError{400, "invalid Content-Length"}
	errDuplicateTE          = &parseError{400, "duplicate Transfer-Encoding"}
	errLengthAndChunked     = &parseError{400, "both Content-Length and Transfer-Encoding"}
	errChunkedHTTP10        = &parseError{400, "Transfer-Encoding in an HTTP/1.0 request"}
	errHeaderTooLarge       = &parseError{431, "request header too large"}
	errVersion              = &parseError{505, "unsupported HTTP version"}
	errUnsupportedTE        = &parseError{501, "unsupported transfer coding"}
	errExpectation          = &parseError{417, "unsupported expectation"}
	errBodyTooLarge         = &parseError{413, "request body too large"}
)

// maxLeadingEmptyLines bounds the empty lines tolerated before a request line (RFC 9112 section 2.2).
const maxLeadingEmptyLines = 4

// reqInfo is the framing and connection semantics of a parsed request.
type reqInfo struct {
	minor          int   // HTTP/1.minor
	contentLength  int64 // -1 when absent
	chunked        bool
	keepAlive      bool
	expectContinue bool
	head           bool
}

// lineReader reads CRLF-terminated lines without copying unless a line is longer than the read buffer.
type lineReader struct {
	br      *bufio.Reader
	scratch []byte
	budget  int // bytes still allowed in this header section
}

func (l *lineReader) readLine() ([]byte, error) {
	line, err := l.br.ReadSlice('\n')
	if errors.Is(err, bufio.ErrBufferFull) {
		l.scratch = append(l.scratch[:0], line...)
		for errors.Is(err, bufio.ErrBufferFull) {
			if len(l.scratch) > l.budget {
				return nil, errHeaderTooLarge
			}
			line, err = l.br.ReadSlice('\n')
			l.scratch = append(l.scratch, line...)
		}
		line = l.scratch
	}
	if err != nil {
		if errors.Is(err, io.EOF) && len(line) > 0 {
			return nil, io.ErrUnexpectedEOF
		}
		return nil, err
	}
	l.budget -= len(line)
	if l.budget < 0 {
		return nil, errHeaderTooLarge
	}
	if len(line) < 2 || line[len(line)-2] != '\r' {
		return nil, errBareLF
	}
	return line[:len(line)-2], nil
}

// parseRequest reads a request line and header section into x.
func parseRequest(lr *lineReader, x exchange.Exchange) (reqInfo, error) {
	info := reqInfo{contentLength: -1}
	var line []byte
	var err error
	for empty := 0; ; empty++ {
		if line, err = lr.readLine(); err != nil {
			return info, err
		}
		if len(line) > 0 {
			break
		}
		if empty == maxLeadingEmptyLines {
			return info, errMalformedRequestLine
		}
	}
	if err := parseRequestLine(line, x, &info); err != nil {
		return info, err
	}

	var hosts, lengths, encodings int
	for {
		if line, err = lr.readLine(); err != nil {
			return info, err
		}
		if len(line) == 0 {
			break
		}
		if line[0] == ' ' || line[0] == '\t' {
			return info, errObsFold
		}
		colon := bytes.IndexByte(line, ':')
		if colon <= 0 {
			return info, errMalformedHeader
		}
		name, value := line[:colon], trimOWS(line[colon+1:])
		if !validFieldValue(value) {
			return info, errMalformedHeader
		}
		switch {
		case equalFold(name, "host"):
			if hosts++; hosts > 1 {
				return info, errDuplicateHost
			}
			if x.SetHost(value) != nil {
				return info, errInvalidHost
			}
			continue
		case equalFold(name, "content-length"):
			if lengths++; lengths > 1 {
				return info, errDuplicateLength
			}
			n, ok := bytesconv.ParseUint(value)
			if !ok {
				return info, errInvalidLength
			}
			info.contentLength = n
		case equalFold(name, "transfer-encoding"):
			if encodings++; encodings > 1 {
				return info, errDuplicateTE
			}
			// Only a bare "chunked" is accepted; anything else is a smuggling vector or unsupported.
			if !equalFold(value, "chunked") {
				return info, errUnsupportedTE
			}
			info.chunked = true
		case equalFold(name, "connection"):
			parseConnection(value, &info)
		case equalFold(name, "expect"):
			if !equalFold(value, "100-continue") {
				return info, errExpectation
			}
			info.expectContinue = info.minor == 1
		}
		if x.AddHeader(name, value) != nil {
			return info, errMalformedHeader
		}
	}
	switch {
	case info.chunked && lengths > 0:
		return info, errLengthAndChunked
	case info.chunked && info.minor == 0:
		return info, errChunkedHTTP10
	case info.minor == 1 && hosts == 0:
		return info, errMissingHost
	}
	return info, nil
}

func parseRequestLine(line []byte, x exchange.Exchange, info *reqInfo) error {
	sp1 := bytes.IndexByte(line, ' ')
	if sp1 <= 0 {
		return errMalformedRequestLine
	}
	method, rest := line[:sp1], line[sp1+1:]
	sp2 := bytes.IndexByte(rest, ' ')
	if sp2 <= 0 {
		return errMalformedRequestLine
	}
	target, version := rest[:sp2], rest[sp2+1:]
	minor, err := parseVersion(version)
	if err != nil {
		return err
	}
	if x.SetMethod(method) != nil {
		return errInvalidMethod
	}
	if x.SetTarget(target) != nil {
		return errInvalidTarget
	}
	x.SetProto(1, minor)
	info.minor = minor
	info.keepAlive = minor == 1
	info.head = string(method) == "HEAD"
	return nil
}

func parseVersion(v []byte) (int, error) {
	if len(v) != 8 || string(v[:5]) != "HTTP/" || v[6] != '.' || !isDigit(v[5]) || !isDigit(v[7]) {
		return 0, errMalformedRequestLine
	}
	if v[5] != '1' || v[7] > '1' {
		return 0, errVersion
	}
	return int(v[7] - '0'), nil
}

// parseConnection applies the close and keep-alive options of a Connection header; close wins.
func parseConnection(v []byte, info *reqInfo) {
	for len(v) > 0 {
		var opt []byte
		opt, v, _ = bytes.Cut(v, []byte(","))
		opt = trimOWS(opt)
		switch {
		case equalFold(opt, "close"):
			info.keepAlive = false
			return
		case equalFold(opt, "keep-alive") && info.minor == 0:
			info.keepAlive = true
		}
	}
}

// validFieldValue allows visible characters, obs-text, SP, and HTAB (RFC 9110 section 5.5).
func validFieldValue(v []byte) bool {
	for _, c := range v {
		if c < ' ' && c != '\t' || c == 0x7f {
			return false
		}
	}
	return true
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

func equalFold(b []byte, lower string) bool {
	return bytesconv.EqualFoldLower(bytesconv.S2B(lower), bytesconv.B2S(b))
}

func isDigit(c byte) bool { return '0' <= c && c <= '9' }
