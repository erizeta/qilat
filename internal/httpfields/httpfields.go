// Package httpfields holds the header field rules HTTP/2 and HTTP/3 share (RFC 9113 section 8, RFC 9114
// section 4) on top of their different header compression.
package httpfields

import (
	"sync/atomic"
	"time"

	"github.com/erizeta/qilat/internal/bytesconv"
	"github.com/erizeta/qilat/internal/exchange"
)

// DefaultUrgency is the RFC 9218 urgency of a request without a priority signal.
const DefaultUrgency = 3

// Request collects and validates the fields of one request header or trailer section as they are decoded,
// filling an exchange. A malformed section is still decoded to the end, since header compression state
// must stay in sync with the peer; Malformed records the first problem.
type Request struct {
	x       exchange.Exchange
	trailer bool
	max     int
	size    int
	// TooLarge is set when the decoded section exceeds the limit (RFC 9113 section 6.5.2), which callers
	// answer with 431.
	TooLarge  bool
	Malformed string

	sawRegular    bool
	method        []byte
	scheme        []byte
	path          []byte
	authority     []byte
	host          []byte
	hasMethod     bool
	hasScheme     bool
	hasPath       bool
	hasAuthority  bool
	hasHost       bool
	cookie        []byte
	ContentLength int64 // -1 if absent
	// ExpectContinue reports "Expect: 100-continue".
	ExpectContinue bool
	Urgency        uint32
	Incremental    bool
}

// Reset prepares r for a new section, keeping its buffers. x may be nil to only validate, as for trailers
// or sections that will be rejected. limit bounds the decoded size.
func (r *Request) Reset(x exchange.Exchange, trailer bool, limit int) {
	*r = Request{
		x: x, trailer: trailer, max: limit, method: r.method[:0], scheme: r.scheme[:0], path: r.path[:0],
		authority: r.authority[:0], host: r.host[:0], cookie: r.cookie[:0], ContentLength: -1,
		Urgency: DefaultUrgency,
	}
}

// Field adds one decoded field. name and value are copied if kept.
func (r *Request) Field(name, value []byte) {
	if r.Malformed != "" || r.TooLarge {
		return
	}
	if r.size += len(name) + len(value) + 32; r.size > r.max {
		r.TooLarge = true
		return
	}
	if len(name) > 0 && name[0] == ':' {
		r.pseudo(name, value)
		return
	}
	r.sawRegular = true
	if !ValidName(name) || !ValidValue(value) {
		r.Malformed = "invalid header field"
		return
	}
	if r.trailer || r.x == nil {
		return // request trailers are validated, then discarded
	}
	switch string(name) {
	case "connection", "keep-alive", "proxy-connection", "transfer-encoding", "upgrade":
		r.Malformed = "connection-specific header field"
		return
	case "te":
		if string(value) != "trailers" {
			r.Malformed = "TE other than trailers"
		}
	case "host":
		r.host, r.hasHost = append(r.host[:0], value...), true
		return
	case "cookie":
		// Cookie crumbs are joined for HTTP/1.1 semantics (RFC 9113 section 8.2.3, RFC 9114 section 4.2.1).
		if len(r.cookie) > 0 {
			r.cookie = append(r.cookie, "; "...)
		}
		r.cookie = append(r.cookie, value...)
		return
	case "content-length":
		n, ok := bytesconv.ParseUint(value)
		if !ok || r.ContentLength >= 0 && r.ContentLength != n {
			r.Malformed = "invalid content-length"
			return
		}
		r.ContentLength = n
	case "expect":
		r.ExpectContinue = bytesconv.EqualFoldLower(continueToken, bytesconv.B2S(value))
	case "priority":
		r.Urgency, r.Incremental = ParsePriority(value, r.Urgency, r.Incremental)
	}
	if r.x.AddHeader(name, value) != nil {
		r.Malformed = "invalid header field"
	}
}

func (r *Request) pseudo(name, value []byte) {
	if r.sawRegular || r.trailer {
		r.Malformed = "misplaced pseudo-header"
		return
	}
	var dst *[]byte
	var seen *bool
	switch string(name) {
	case ":method":
		dst, seen = &r.method, &r.hasMethod
	case ":scheme":
		dst, seen = &r.scheme, &r.hasScheme
	case ":path":
		dst, seen = &r.path, &r.hasPath
	case ":authority":
		dst, seen = &r.authority, &r.hasAuthority
	default:
		r.Malformed = "unknown pseudo-header"
		return
	}
	if *seen {
		r.Malformed = "duplicate pseudo-header"
		return
	}
	*dst, *seen = append((*dst)[:0], value...), true
}

// Finish validates the pseudo-headers (RFC 9113 section 8.3.1, RFC 9114 section 4.3.1) and sets the
// request line, host, cookie, and protocol version on the exchange. It reports why the request is
// malformed, or "" if it is valid.
func (r *Request) Finish(major int) string {
	if r.Malformed == "" {
		r.Malformed = r.validate()
	}
	if r.Malformed != "" {
		return r.Malformed
	}
	target := r.path
	if string(r.method) == "CONNECT" {
		target = r.authority
	}
	authority := r.authority
	if !r.hasAuthority {
		authority = r.host
	}
	x := r.x
	if x.SetMethod(r.method) != nil || x.SetTarget(target) != nil || x.SetHost(authority) != nil {
		return "invalid request pseudo-header"
	}
	if len(r.cookie) > 0 && x.AddHeader(cookieName, r.cookie) != nil {
		return "invalid cookie"
	}
	x.SetProto(major, 0)
	return ""
}

// Method returns the :method value. It is valid until the next Reset.
func (r *Request) Method() []byte { return r.method }

var (
	cookieName    = []byte("cookie")
	continueToken = []byte("100-continue")
)

func (r *Request) validate() string {
	switch {
	case !r.hasMethod:
		return "missing :method"
	case string(r.method) == "CONNECT":
		if !r.hasAuthority || r.hasScheme || r.hasPath {
			return "invalid CONNECT pseudo-headers"
		}
		return ""
	case !r.hasScheme || !r.hasPath || len(r.path) == 0:
		return "missing :scheme or :path"
	case r.path[0] != '/' && (string(r.path) != "*" || string(r.method) != "OPTIONS"):
		return "invalid :path"
	case r.hasAuthority && r.hasHost && string(r.authority) != string(r.host):
		return ":authority and host differ"
	}
	return ""
}

// ValidName reports whether name is a lowercase RFC 9110 token, as HTTP/2 and HTTP/3 require.
func ValidName(name []byte) bool {
	if len(name) == 0 {
		return false
	}
	for _, ch := range name {
		if !lowerTokenTable[ch] {
			return false
		}
	}
	return true
}

var lowerTokenTable = func() (t [256]bool) {
	for c := range 256 {
		switch {
		case 'a' <= c && c <= 'z', '0' <= c && c <= '9':
			t[c] = true
		}
	}
	for _, c := range "!#$%&'*+-.^_`|~" {
		t[c] = true
	}
	return t
}()

// ValidValue rejects NUL, CR, LF, and leading or trailing whitespace (RFC 9113 section 8.2.1).
func ValidValue(v []byte) bool {
	for _, ch := range v {
		if ch == 0 || ch == '\r' || ch == '\n' {
			return false
		}
	}
	return len(v) == 0 || v[0] != ' ' && v[0] != '\t' && v[len(v)-1] != ' ' && v[len(v)-1] != '\t'
}

// SkipResponse reports response fields HTTP/2 and HTTP/3 must not send (RFC 9113 section 8.2.2), plus,
// in trailers, fields that only make sense in a header section.
func SkipResponse(name []byte, trailer bool) bool {
	switch string(name) {
	case "connection", "keep-alive", "proxy-connection", "transfer-encoding", "upgrade":
		return true
	case "content-length", "host", "te", "trailer":
		return trailer
	}
	return false
}

// ParsePriority reads urgency and incremental from an RFC 9218 Priority field value such as "u=1, i".
// Unknown or malformed members are ignored, as the RFC requires.
func ParsePriority(v []byte, urgency uint32, incremental bool) (uint32, bool) {
	for len(v) > 0 {
		var member []byte
		member, v = cutByte(v, ',')
		item, _ := cutByte(trimSpace(member), ';') // parameters are ignored
		key, val := cutByte(item, '=')
		switch string(key) {
		case "u":
			if len(val) == 1 && '0' <= val[0] && val[0] <= '7' {
				urgency = uint32(val[0] - '0')
			}
		case "i":
			switch {
			case len(val) == 0 && len(item) == 1, string(val) == "?1":
				incremental = true
			case string(val) == "?0":
				incremental = false
			}
		}
	}
	return urgency, incremental
}

func cutByte(b []byte, sep byte) (before, after []byte) {
	for i, c := range b {
		if c == sep {
			return b[:i], b[i+1:]
		}
	}
	return b, nil
}

func trimSpace(b []byte) []byte {
	for len(b) > 0 && (b[0] == ' ' || b[0] == '\t') {
		b = b[1:]
	}
	for len(b) > 0 && (b[len(b)-1] == ' ' || b[len(b)-1] == '\t') {
		b = b[:len(b)-1]
	}
	return b
}

// DateName is the lowercase name of the Date field.
var DateName = []byte("date")

var dateCache atomic.Pointer[dateEntry]

type dateEntry struct {
	sec   int64
	value []byte
}

// Date returns the current time in HTTP-date format, cached per second. The result must not be modified.
func Date() []byte {
	now := time.Now()
	sec := now.Unix()
	if d := dateCache.Load(); d != nil && d.sec == sec {
		return d.value
	}
	v := now.UTC().AppendFormat(nil, "Mon, 02 Jan 2006 15:04:05 GMT")
	dateCache.Store(&dateEntry{sec: sec, value: v})
	return v
}
