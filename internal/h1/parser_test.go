package h1

import (
	"bufio"
	"bytes"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"

	"github.com/erizeta/qilat/internal/exchange"
)

// recorder is an exchange that records what the parser sets.
type recorder struct {
	method, target, host string
	major, minor         int
	headers              [][2]string
	failMethod           bool
}

var _ exchange.Exchange = (*recorder)(nil)

func (r *recorder) SetMethod(m []byte) error {
	if r.failMethod || !validToken(m) {
		return errors.New("bad method")
	}
	r.method = string(m)
	return nil
}

func (r *recorder) SetTarget(t []byte) error {
	if len(t) == 0 || (t[0] != '/' && string(t) != "*" && !bytes.Contains(t, []byte("://"))) {
		return errors.New("bad target")
	}
	r.target = string(t)
	return nil
}

func (r *recorder) SetHost(h []byte) error {
	if bytes.ContainsAny(h, " /?#@") {
		return errors.New("bad host")
	}
	r.host = string(h)
	return nil
}

func (r *recorder) SetProto(major, minor int) { r.major, r.minor = major, minor }

func (r *recorder) AddHeader(name, value []byte) error {
	if !validToken(name) {
		return errors.New("bad name")
	}
	r.headers = append(r.headers, [2]string{strings.ToLower(string(name)), string(value)})
	return nil
}

func (r *recorder) SetBody(io.Reader, int64)                        {}
func (r *recorder) SetConn(net.Addr, *tls.ConnectionState)          {}
func (r *recorder) SetDisconnectWatcher(exchange.DisconnectWatcher) {}
func (r *recorder) Serve(exchange.ResponseSink) error               { return nil }
func (r *recorder) Cancel()                                         {}
func (r *recorder) Release()                                        {}

func parse(raw string, maxHeader int) (*recorder, reqInfo, error) {
	rec := &recorder{}
	lr := lineReader{br: bufio.NewReaderSize(strings.NewReader(raw), 64), budget: maxHeader}
	info, err := parseRequest(&lr, rec)
	return rec, info, err
}

func TestParseRequest(t *testing.T) {
	rec, info, err := parse("\r\nPOST /upload?x=1 HTTP/1.1\r\nHost: example.com\r\nContent-Length: 12\r\n"+
		"X-Pad:   spaced value \t\r\nConnection: keep-alive, Upgrade\r\nExpect: 100-continue\r\n\r\n", 1<<16)
	if err != nil {
		t.Fatal(err)
	}
	if rec.method != "POST" || rec.target != "/upload?x=1" || rec.host != "example.com" || rec.major != 1 || rec.minor != 1 {
		t.Fatalf("recorded %+v", rec)
	}
	if info.contentLength != 12 || info.chunked || !info.keepAlive || !info.expectContinue || info.head {
		t.Fatalf("info %+v", info)
	}
	want := [][2]string{{"content-length", "12"}, {"x-pad", "spaced value"}, {"connection", "keep-alive, Upgrade"}, {"expect", "100-continue"}}
	if len(rec.headers) != len(want) {
		t.Fatalf("headers %q", rec.headers)
	}
	for i := range want {
		if rec.headers[i] != want[i] {
			t.Errorf("header %d = %q, want %q", i, rec.headers[i], want[i])
		}
	}
}

func TestParseRequestSemantics(t *testing.T) {
	tests := []struct {
		name, raw string
		want      reqInfo
	}{
		{"http10 default close", "GET / HTTP/1.0\r\n\r\n", reqInfo{minor: 0, contentLength: -1}},
		{"http10 keep-alive", "GET / HTTP/1.0\r\nConnection: Keep-Alive\r\n\r\n", reqInfo{minor: 0, contentLength: -1, keepAlive: true}},
		{"http11 close", "GET / HTTP/1.1\r\nHost: h\r\nConnection: upgrade, close\r\n\r\n", reqInfo{minor: 1, contentLength: -1}},
		{"chunked", "POST / HTTP/1.1\r\nHost: h\r\nTransfer-Encoding: Chunked\r\n\r\n", reqInfo{minor: 1, contentLength: -1, chunked: true, keepAlive: true}},
		{"head", "HEAD / HTTP/1.1\r\nHost: h\r\n\r\n", reqInfo{minor: 1, contentLength: -1, keepAlive: true, head: true}},
		{"expect ignored in 1.0", "PUT / HTTP/1.0\r\nExpect: 100-continue\r\nContent-Length: 1\r\n\r\n", reqInfo{minor: 0, contentLength: 1}},
		{"zero length", "POST / HTTP/1.1\r\nHost: h\r\nContent-Length: 0\r\n\r\n", reqInfo{minor: 1, contentLength: 0, keepAlive: true}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, info, err := parse(tt.raw, 1<<16)
			if err != nil {
				t.Fatal(err)
			}
			if info != tt.want {
				t.Fatalf("info = %+v, want %+v", info, tt.want)
			}
		})
	}
}

// TestParseRequestRejects is the request smuggling and malformed-input suite.
func TestParseRequestRejects(t *testing.T) {
	const host = "Host: h\r\n"
	tests := []struct {
		name, raw string
		want      *parseError
	}{
		{"CL and TE", "POST / HTTP/1.1\r\n" + host + "Content-Length: 5\r\nTransfer-Encoding: chunked\r\n\r\n", errLengthAndChunked},
		{"TE and CL", "POST / HTTP/1.1\r\n" + host + "Transfer-Encoding: chunked\r\nContent-Length: 5\r\n\r\n", errLengthAndChunked},
		{"duplicate CL same value", "POST / HTTP/1.1\r\n" + host + "Content-Length: 5\r\nContent-Length: 5\r\n\r\n", errDuplicateLength},
		{"duplicate CL different", "POST / HTTP/1.1\r\n" + host + "Content-Length: 5\r\nContent-Length: 6\r\n\r\n", errDuplicateLength},
		{"CL list", "POST / HTTP/1.1\r\n" + host + "Content-Length: 5, 5\r\n\r\n", errInvalidLength},
		{"CL negative", "POST / HTTP/1.1\r\n" + host + "Content-Length: -1\r\n\r\n", errInvalidLength},
		{"CL plus sign", "POST / HTTP/1.1\r\n" + host + "Content-Length: +5\r\n\r\n", errInvalidLength},
		{"CL hex", "POST / HTTP/1.1\r\n" + host + "Content-Length: 0x5\r\n\r\n", errInvalidLength},
		{"CL overflow", "POST / HTTP/1.1\r\n" + host + "Content-Length: 99999999999999999999\r\n\r\n", errInvalidLength},
		{"CL empty", "POST / HTTP/1.1\r\n" + host + "Content-Length: \r\n\r\n", errInvalidLength},
		{"TE list", "POST / HTTP/1.1\r\n" + host + "Transfer-Encoding: gzip, chunked\r\n\r\n", errUnsupportedTE},
		{"TE chunked then gzip", "POST / HTTP/1.1\r\n" + host + "Transfer-Encoding: chunked, gzip\r\n\r\n", errUnsupportedTE},
		{"TE obfuscated", "POST / HTTP/1.1\r\n" + host + "Transfer-Encoding: xchunked\r\n\r\n", errUnsupportedTE},
		{"TE vertical tab", "POST / HTTP/1.1\r\n" + host + "Transfer-Encoding: chunked\v\r\n\r\n", errMalformedHeader},
		{"duplicate TE", "POST / HTTP/1.1\r\n" + host + "Transfer-Encoding: chunked\r\nTransfer-Encoding: chunked\r\n\r\n", errDuplicateTE},
		{"TE in HTTP/1.0", "POST / HTTP/1.0\r\nTransfer-Encoding: chunked\r\n\r\n", errChunkedHTTP10},
		{"space before colon", "GET / HTTP/1.1\r\n" + host + "Transfer-Encoding : chunked\r\n\r\n", errMalformedHeader},
		{"obs-fold", "GET / HTTP/1.1\r\n" + host + "X-A: 1\r\n continued\r\n\r\n", errObsFold},
		{"bare LF request line", "GET / HTTP/1.1\n" + host + "\r\n", errBareLF},
		{"bare LF header", "GET / HTTP/1.1\r\nHost: h\n\r\n", errBareLF},
		{"bare CR in value", "GET / HTTP/1.1\r\n" + host + "X-A: a\rb\r\n\r\n", errMalformedHeader},
		{"NUL in value", "GET / HTTP/1.1\r\n" + host + "X-A: a\x00b\r\n\r\n", errMalformedHeader},
		{"no colon", "GET / HTTP/1.1\r\n" + host + "X-A\r\n\r\n", errMalformedHeader},
		{"empty name", "GET / HTTP/1.1\r\n" + host + ": v\r\n\r\n", errMalformedHeader},
		{"missing host", "GET / HTTP/1.1\r\n\r\n", errMissingHost},
		{"duplicate host", "GET / HTTP/1.1\r\nHost: a\r\nHost: b\r\n\r\n", errDuplicateHost},
		{"invalid host", "GET / HTTP/1.1\r\nHost: a/b\r\n\r\n", errInvalidHost},
		{"HTTP/1.2", "GET / HTTP/1.2\r\n" + host + "\r\n", errVersion},
		{"HTTP/2.0", "GET / HTTP/2.0\r\n" + host + "\r\n", errVersion},
		{"lowercase version", "GET / http/1.1\r\n" + host + "\r\n", errMalformedRequestLine},
		{"version suffix", "GET / HTTP/1.1x\r\n" + host + "\r\n", errMalformedRequestLine},
		{"double space", "GET  / HTTP/1.1\r\n" + host + "\r\n", errMalformedRequestLine},
		{"no target", "GET HTTP/1.1\r\n" + host + "\r\n", errMalformedRequestLine},
		{"space in target", "GET /a b HTTP/1.1\r\n" + host + "\r\n", errMalformedRequestLine},
		{"invalid method", "G(T / HTTP/1.1\r\n" + host + "\r\n", errInvalidMethod},
		{"invalid target", "GET a HTTP/1.1\r\n" + host + "\r\n", errInvalidTarget},
		{"bad expectation", "GET / HTTP/1.1\r\n" + host + "Expect: 200-ok\r\n\r\n", errExpectation},
		{"too many empty lines", "\r\n\r\n\r\n\r\n\r\nGET / HTTP/1.1\r\n" + host + "\r\n", errMalformedRequestLine},
		{"header too large", "GET / HTTP/1.1\r\n" + host + "X-Big: " + strings.Repeat("a", 300) + "\r\n\r\n", errHeaderTooLarge},
		{"line longer than buffer and budget", "GET /" + strings.Repeat("a", 400) + " HTTP/1.1\r\n\r\n", errHeaderTooLarge},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, err := parse(tt.raw, 256)
			if !errors.Is(err, tt.want) {
				t.Fatalf("err = %v, want %v", err, tt.want)
			}
		})
	}
}

func TestParseRequestLongLineWithinBudget(t *testing.T) {
	// Lines longer than the 64-byte test buffer must still parse when within MaxHeaderBytes.
	value := strings.Repeat("v", 1000)
	rec, _, err := parse("GET /"+strings.Repeat("p", 500)+" HTTP/1.1\r\nHost: h\r\nX-Long: "+value+"\r\n\r\n", 1<<16)
	if err != nil {
		t.Fatal(err)
	}
	if len(rec.target) != 501 || rec.headers[0][1] != value {
		t.Fatalf("target len %d header len %d", len(rec.target), len(rec.headers[0][1]))
	}
}

func TestParseRequestTruncated(t *testing.T) {
	for _, raw := range []string{"", "GET / HTTP/1.1\r\nHost: h\r\n", "GET / HTTP/1.1\r\nHost"} {
		_, _, err := parse(raw, 1<<16)
		var pe *parseError
		if err == nil || errors.As(err, &pe) {
			t.Errorf("parse(%q) err = %v, want an I/O error", raw, err)
		}
	}
}

func TestParseRequestExchangeErrors(t *testing.T) {
	rec := &recorder{failMethod: true}
	lr := lineReader{br: bufio.NewReader(strings.NewReader("GET / HTTP/1.1\r\nHost: h\r\n\r\n")), budget: 1 << 16}
	if _, err := parseRequest(&lr, rec); !errors.Is(err, errInvalidMethod) {
		t.Fatalf("err = %v", err)
	}
}

// FuzzParseRequest checks that every request the parser accepts is also accepted by net/http with the same meaning.
func FuzzParseRequest(f *testing.F) {
	for _, s := range []string{
		"GET / HTTP/1.1\r\nHost: h\r\n\r\n",
		"POST /p?q=1 HTTP/1.1\r\nHost: h\r\nContent-Length: 3\r\n\r\nabc",
		"POST / HTTP/1.1\r\nHost: h\r\nTransfer-Encoding: chunked\r\n\r\n3\r\nabc\r\n0\r\n\r\n",
		"GET / HTTP/1.0\r\nConnection: keep-alive\r\n\r\n",
		"POST / HTTP/1.1\r\nHost: h\r\nContent-Length: 5\r\nTransfer-Encoding: chunked\r\n\r\n",
	} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, raw string) {
		rec, info, err := parse(raw, 1<<16)
		if err != nil {
			return
		}
		req, stdErr := http.ReadRequest(bufio.NewReader(strings.NewReader(raw)))
		if stdErr != nil {
			t.Fatalf("qilat accepted, net/http rejected (%v):\n%q", stdErr, raw)
		}
		if req.Method != rec.method || req.RequestURI != rec.target {
			t.Fatalf("method/target %q %q, net/http %q %q", rec.method, rec.target, req.Method, req.RequestURI)
		}
		if info.chunked != (len(req.TransferEncoding) > 0) {
			t.Fatalf("chunked %v, net/http TE %q", info.chunked, req.TransferEncoding)
		}
		if !info.chunked && info.contentLength >= 0 && req.ContentLength != info.contentLength {
			t.Fatalf("content length %d, net/http %d", info.contentLength, req.ContentLength)
		}
		if rec.minor == 1 && req.Host != rec.host {
			t.Fatalf("host %q, net/http %q", rec.host, req.Host)
		}
		for _, h := range rec.headers {
			// net/http moves Transfer-Encoding out of Header; framing is compared above.
			if h[0] == "transfer-encoding" {
				continue
			}
			if !containsValue(req.Header.Values(h[0]), h[1]) {
				t.Fatalf("header %s=%q missing from net/http %q", h[0], h[1], req.Header.Values(h[0]))
			}
		}
	})
}

func containsValue(vs []string, v string) bool {
	for _, x := range vs {
		if x == v {
			return true
		}
	}
	return false
}
