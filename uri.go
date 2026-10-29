package qilat

import (
	"bytes"
	"errors"

	"github.com/erizeta/qilat/internal/bytesconv"
)

// ErrInvalidURI is returned when a request-target is malformed.
var ErrInvalidURI = errors.New("qilat: invalid request URI")

type targetForm uint8

const (
	formOrigin    targetForm = iota // /path?query
	formAbsolute                    // scheme://host/path?query
	formAuthority                   // host:port, used by CONNECT
	formAsterisk                    // *, used by server-wide OPTIONS
)

// URI is a parsed request-target (RFC 9112 section 3.2). Byte slices it returns are valid
// until the next Parse or until the handler returns. The zero value is an empty origin-form target.
type URI struct {
	raw      []byte
	form     targetForm
	scheme   []byte
	host     []byte
	rawPath  []byte
	rawQuery []byte

	path      []byte // decoded path; may alias raw
	pathBuf   []byte // owns decoded bytes so decoding never writes into raw
	pathReady bool
	query     Args
	queryDone bool
}

// Parse copies target and parses it in origin, absolute, authority, or asterisk form.
// Path decoding and query parsing are deferred until [URI.Path] and [URI.Query] are called.
func (u *URI) Parse(target []byte) error {
	u.reset()
	u.raw = append(u.raw[:0], target...)
	t := u.raw
	if len(t) == 0 {
		return ErrInvalidURI
	}
	for _, c := range t {
		if c <= ' ' || c == 0x7f || c == '#' {
			return ErrInvalidURI
		}
	}
	switch {
	case t[0] == '/':
		u.form = formOrigin
		return u.splitPathQuery(t)
	case len(t) == 1 && t[0] == '*':
		u.form = formAsterisk
		u.rawPath = t
		u.path = t
		u.pathReady = true
		return nil
	}
	if i := bytes.Index(t, []byte("://")); i > 0 && validScheme(t[:i]) {
		return u.parseAbsolute(t, i)
	}
	if bytes.ContainsAny(t, "/?@") {
		return ErrInvalidURI
	}
	u.form = formAuthority
	u.host = t
	return nil
}

func (u *URI) parseAbsolute(t []byte, schemeEnd int) error {
	u.form = formAbsolute
	u.scheme = t[:schemeEnd]
	rest := t[schemeEnd+3:]
	end := bytes.IndexAny(rest, "/?")
	if end < 0 {
		end = len(rest)
	}
	u.host = rest[:end]
	// Userinfo in a request-target is deprecated (RFC 9110 section 4.2.4) and a known phishing vector.
	if len(u.host) == 0 || bytes.IndexByte(u.host, '@') >= 0 {
		return ErrInvalidURI
	}
	rest = rest[end:]
	if len(rest) == 0 || rest[0] == '?' {
		if err := u.splitPathQuery(rest); err != nil {
			return err
		}
		u.rawPath = rootPath
		return nil
	}
	return u.splitPathQuery(rest)
}

var rootPath = []byte("/")

func (u *URI) splitPathQuery(t []byte) error {
	p := t
	if i := bytes.IndexByte(t, '?'); i >= 0 {
		p, u.rawQuery = t[:i], t[i+1:]
	}
	if !validEscapes(p) {
		return ErrInvalidURI
	}
	u.rawPath = p
	return nil
}

func validScheme(s []byte) bool {
	if !isAlpha(s[0]) {
		return false
	}
	for _, c := range s[1:] {
		if !isSchemeChar(c) {
			return false
		}
	}
	return true
}

func isSchemeChar(c byte) bool {
	return isAlpha(c) || '0' <= c && c <= '9' || c == '+' || c == '-' || c == '.'
}

func isAlpha(c byte) bool { return 'a' <= c && c <= 'z' || 'A' <= c && c <= 'Z' }

func validEscapes(s []byte) bool {
	for i := 0; i < len(s); i++ {
		if s[i] != '%' {
			continue
		}
		if i+2 >= len(s) {
			return false
		}
		if _, ok := bytesconv.Unhex(s[i+1]); !ok {
			return false
		}
		if _, ok := bytesconv.Unhex(s[i+2]); !ok {
			return false
		}
		i += 2
	}
	return true
}

// Raw returns the request-target exactly as parsed.
func (u *URI) Raw() []byte { return u.raw }

// Scheme returns the scheme of an absolute-form target, or nil for other forms.
func (u *URI) Scheme() []byte { return u.scheme }

// Host returns the authority of an absolute- or authority-form target, or nil for other forms.
func (u *URI) Host() []byte { return u.host }

// RawPath returns the path still percent-encoded. It is empty for authority-form targets.
func (u *URI) RawPath() []byte {
	if u.form == formOrigin && len(u.rawPath) == 0 {
		return rootPath
	}
	return u.rawPath
}

// Path returns the percent-decoded path. An encoded slash (%2F) decodes to '/';
// use [URI.RawPath] when the distinction matters. '+' is not decoded in paths.
func (u *URI) Path() []byte {
	if !u.pathReady {
		u.path = u.decodePath()
		u.pathReady = true
	}
	return u.path
}

func (u *URI) decodePath() []byte {
	raw := u.RawPath()
	if bytes.IndexByte(raw, '%') < 0 {
		return raw
	}
	u.pathBuf = appendUnescape(u.pathBuf[:0], raw, false)
	return u.pathBuf
}

// RawQuery returns the query without the leading '?', still percent-encoded.
func (u *URI) RawQuery() []byte { return u.rawQuery }

// Query returns the parsed query arguments.
func (u *URI) Query() *Args {
	if !u.queryDone {
		u.query.Parse(u.rawQuery)
		u.queryDone = true
	}
	return &u.query
}

// String returns the request-target as a string. It allocates.
func (u *URI) String() string {
	if len(u.raw) == 0 {
		return "/"
	}
	return string(u.raw)
}

// CopyTo makes dst an independent copy of u.
func (u *URI) CopyTo(dst *URI) {
	// raw was validated when u was parsed, so reparsing cannot fail.
	_ = dst.Parse(u.raw)
	if len(u.raw) == 0 {
		dst.reset()
	}
}

func (u *URI) reset() {
	u.form = formOrigin
	u.raw = u.raw[:0]
	u.scheme, u.host, u.rawPath, u.rawQuery = nil, nil, nil, nil
	u.pathReady = false
	u.queryDone = false
}

// appendUnescape appends the percent-decoding of s to dst. Invalid escapes are copied verbatim.
func appendUnescape(dst, s []byte, plusIsSpace bool) []byte {
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c == '%' && i+2 < len(s):
			hi, ok1 := bytesconv.Unhex(s[i+1])
			lo, ok2 := bytesconv.Unhex(s[i+2])
			if ok1 && ok2 {
				dst = append(dst, hi<<4|lo)
				i += 2
				continue
			}
		case c == '+' && plusIsSpace:
			c = ' '
		}
		dst = append(dst, c)
	}
	return dst
}
