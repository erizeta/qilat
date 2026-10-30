package qilat

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/erizeta/qilat/internal/bytesconv"
	"github.com/erizeta/qilat/internal/exchange"
)

// RoundTripper sends one request and receives its response, like [net/http.RoundTripper].
// The response body is read through resp afterwards, and resp must be released or closed when done.
type RoundTripper interface {
	RoundTrip(req *Request, resp *Response) error
}

// Client sends HTTP requests over HTTP/1.1, HTTP/2, and HTTP/3. Requests and responses are pooled: get them
// with [AcquireRequest] and [AcquireResponse] and give them back with [ReleaseRequest] and [ReleaseResponse].
// A Client is safe for concurrent use.
type Client struct {
	// Transport sends requests; nil means DefaultTransport.
	Transport RoundTripper
	// Timeout limits the whole request, including reading the response body; zero means no limit.
	// The request context, set with Request.WithContext, can end it sooner.
	Timeout time.Duration
	// CheckRedirect decides whether to follow a redirect response; redirects counts those already followed.
	// It sees the request just sent and its redirect response. Returning ErrUseLastResponse makes Do return
	// that response; any other error is returned by Do. Nil means follow up to 10 redirects.
	CheckRedirect func(req *Request, resp *Response, redirects int) error
}

// DefaultClient is the Client used by [Get].
var DefaultClient = &Client{}

var (
	// ErrUseLastResponse can be returned by Client.CheckRedirect to stop at the redirect response.
	ErrUseLastResponse = errors.New("qilat: use last response")
	// ErrTooManyRedirects is returned by Client.Do after 10 redirects when CheckRedirect is nil.
	ErrTooManyRedirects = errors.New("qilat: stopped after 10 redirects")
	// ErrResponseBodyTooLarge is returned by Response.ReadBody when the body exceeds Transport.MaxResponseBodySize.
	ErrResponseBodyTooLarge = errors.New("qilat: response body too large")
	errClientURI            = errors.New("qilat: request URI must be absolute, with an http or https scheme and a host")
)

// Get fetches url with [DefaultClient] into resp.
func Get(url string, resp *Response) error { return DefaultClient.Get(url, resp) }

// Get fetches url into resp.
func (c *Client) Get(url string, resp *Response) error {
	req := AcquireRequest()
	defer ReleaseRequest(req)
	if err := req.SetRequestURI(url); err != nil {
		return err
	}
	return c.Do(req, resp)
}

// Do sends req and fills resp, following redirects. The response body streams from resp.Body until
// resp is released or closed. After a followed redirect, req describes the last request sent.
func (c *Client) Do(req *Request, resp *Response) error {
	req.live()
	rt := c.Transport
	if rt == nil {
		rt = DefaultTransport
	}
	outer := req.ctx
	var cancel context.CancelFunc
	if c.Timeout > 0 {
		req.ctx, cancel = context.WithTimeout(req.clientContext(), c.Timeout)
	}
	defer func() { req.ctx = outer }()

	for redirects := 0; ; redirects++ {
		resp.reset()
		if err := rt.RoundTrip(req, resp); err != nil {
			if cancel != nil {
				cancel()
			}
			return err
		}
		resp.cancel = cancel
		loc := resp.Header.Peek("location")
		if !isRedirect(resp.status) || len(loc) == 0 {
			return nil
		}
		var err error
		switch {
		case c.CheckRedirect != nil:
			err = c.CheckRedirect(req, resp, redirects)
		case redirects >= 10:
			err = ErrTooManyRedirects
		}
		if errors.Is(err, ErrUseLastResponse) {
			return nil
		}
		if err != nil {
			return err
		}
		follow, err := req.prepareRedirect(resp.status, loc)
		if err != nil || !follow {
			return err
		}
		resp.cancel = nil // the next response takes the timeout over
		_ = resp.Close()
	}
}

func isRedirect(status int) bool {
	switch status {
	case 301, 302, 303, 307, 308:
		return true
	}
	return false
}

// prepareRedirect turns r into the request that follows a redirect to loc. It reports false when the
// redirect cannot be followed because the body cannot be sent again.
func (r *Request) prepareRedirect(status int, loc []byte) (bool, error) {
	base, err := url.Parse(r.uri.String())
	if err != nil {
		return false, err
	}
	ref, err := url.Parse(string(loc))
	if err != nil {
		return false, fmt.Errorf("qilat: invalid redirect location %q: %w", loc, err)
	}
	next := base.ResolveReference(ref)
	keepBody := status == 307 || status == 308
	if keepBody {
		if _, ok := r.Body.(noBody); !ok {
			s, ok := r.Body.(io.Seeker)
			if !ok {
				return false, nil
			}
			if _, err := s.Seek(0, io.SeekStart); err != nil {
				return false, nil
			}
		}
	} else {
		if r.method != "GET" && r.method != "HEAD" {
			r.method = "GET"
		}
		r.Body = noBody{}
		r.contentLength = -1
		r.header.Del("content-type")
		r.header.Del("content-length")
	}
	oldHost, wasHTTPS := string(r.uri.Host()), string(r.uri.Scheme()) == "https"
	if err := r.SetRequestURI(next.String()); err != nil {
		return false, err
	}
	// Credentials meant for one host must not be sent to another, nor in cleartext after a downgrade.
	if string(r.uri.Host()) != oldHost || wasHTTPS && string(r.uri.Scheme()) != "https" {
		r.header.Del("authorization")
		r.header.Del("www-authenticate")
		r.header.Del("cookie")
		r.header.Del("proxy-authorization")
	}
	return true, nil
}

var requestPool = sync.Pool{New: func() any { return new(Request) }}

// AcquireRequest returns an empty client request from a pool. Give it back with [ReleaseRequest].
func AcquireRequest() *Request {
	r := requestPool.Get().(*Request)
	r.init(context.Background())
	r.method = "GET"
	return r
}

// ReleaseRequest returns r to the pool. r must not be used afterwards.
func ReleaseRequest(r *Request) {
	r.reset()
	r.contentLength = -1
	requestPool.Put(r)
}

// SetMethod sets the request method. It panics if method is not an RFC 9110 token.
func (r *Request) SetMethod(method string) {
	r.live()
	if err := r.setMethod(bytesconv.S2B(method)); err != nil {
		panic("qilat: invalid method " + method)
	}
}

// SetRequestURI sets the URL a client request is sent to, such as "https://example.com/path?q=1",
// and its Host header.
func (r *Request) SetRequestURI(uri string) error {
	r.live()
	// The fragment is for the client only (RFC 9110 section 4.2.4).
	if i := strings.IndexByte(uri, '#'); i >= 0 {
		uri = uri[:i]
	}
	if err := r.uri.Parse(bytesconv.S2B(uri)); err != nil {
		return err
	}
	scheme := r.uri.Scheme()
	if r.uri.form != formAbsolute || !equalFold(scheme, "http") && !equalFold(scheme, "https") {
		return errClientURI
	}
	lowerInto(scheme[:0], scheme)
	return r.setHost(r.uri.Host())
}

// SetHost overrides the Host header that SetRequestURI set.
func (r *Request) SetHost(host string) error {
	r.live()
	return r.setHost(bytesconv.S2B(host))
}

// SetBody sets the request body. contentLength is -1 when unknown, which sends a chunked body on HTTP/1.1.
// A body that implements io.Seeker can be sent again after a 307 or 308 redirect.
func (r *Request) SetBody(body io.Reader, contentLength int64) {
	r.live()
	if body == nil {
		body, contentLength = noBody{}, -1
	}
	r.Body = body
	r.contentLength = contentLength
}

// clientContext returns the context of a client request without allocating the server's cancelable one.
func (r *Request) clientContext() context.Context {
	if r.ctx != nil {
		return r.ctx
	}
	if r.baseCtx != nil {
		return r.baseCtx
	}
	return context.Background()
}

// DefaultMaxResponseBodySize is the limit of Response.ReadBody when Transport.MaxResponseBodySize is zero.
const DefaultMaxResponseBodySize = 64 << 20

// Response is an HTTP response received by a [Client]. It is pooled: the Response and every byte slice it
// returns are valid until [ReleaseResponse].
type Response struct {
	// Header holds the response header fields.
	Header *Header
	// Body streams the response body. It is never nil. Read it to EOF, or release the response, so the
	// connection can be reused.
	Body io.Reader

	header        Header
	trailer       Header
	status        int
	proto         string
	contentLength int64
	body          io.ReadCloser // the protocol's reader; nil once it has returned EOF or an error
	bodyErr       error
	rbody         responseBody
	maxBody       int64
	buf           []byte
	limited       io.LimitedReader // kept here so ReadBody does not allocate
	cancel        context.CancelFunc
}

var responsePool = sync.Pool{New: func() any { return new(Response) }}

// AcquireResponse returns an empty response from a pool. Give it back with [ReleaseResponse].
func AcquireResponse() *Response {
	resp := responsePool.Get().(*Response)
	resp.reset()
	return resp
}

// ReleaseResponse closes resp's body and returns resp to the pool. resp must not be used afterwards.
func ReleaseResponse(resp *Response) {
	_ = resp.Close()
	resp.reset()
	if cap(resp.buf) > 1<<20 {
		resp.buf = nil
	}
	responsePool.Put(resp)
}

func (resp *Response) reset() {
	resp.rbody.resp = resp
	resp.bodyErr = nil
	resp.Header = &resp.header
	resp.Body = noBody{}
	resp.header.Reset()
	resp.trailer.Reset()
	resp.status = 0
	resp.proto = ""
	resp.contentLength = -1
	resp.body = nil
	resp.maxBody = 0
	resp.buf = resp.buf[:0]
	resp.cancel = nil
}

// StatusCode returns the status code, such as 200.
func (resp *Response) StatusCode() int { return resp.status }

// Proto returns the protocol version: "HTTP/1.1", "HTTP/2.0", or "HTTP/3.0".
func (resp *Response) Proto() string { return resp.proto }

// ContentLength returns the declared body length, or -1 if it is unknown.
func (resp *Response) ContentLength() int64 { return resp.contentLength }

// Trailer returns the trailer fields, which are complete once Body has returned io.EOF.
func (resp *Response) Trailer() *Header { return &resp.trailer }

// ReadBody reads the whole body into a buffer owned by resp and returns it. It fails with
// [ErrResponseBodyTooLarge] past Transport.MaxResponseBodySize.
func (resp *Response) ReadBody() ([]byte, error) {
	limit := resp.maxBody
	if limit == 0 {
		limit = DefaultMaxResponseBodySize
	}
	if resp.contentLength > limit && limit > 0 {
		return nil, ErrResponseBodyTooLarge
	}
	buf := bytes.NewBuffer(resp.buf[:0])
	if resp.contentLength > 0 {
		// The declared length is the server's word; grow further only as bytes arrive.
		buf.Grow(int(min(resp.contentLength, 1<<20)))
	}
	src := io.Reader(resp.Body)
	if limit > 0 {
		resp.limited = io.LimitedReader{R: resp.Body, N: limit + 1}
		src = &resp.limited
	}
	_, err := buf.ReadFrom(src)
	resp.limited = io.LimitedReader{}
	resp.buf = buf.Bytes()
	if err != nil {
		return nil, err
	}
	if limit > 0 && int64(len(resp.buf)) > limit {
		_ = resp.Close()
		return nil, ErrResponseBodyTooLarge
	}
	return resp.buf, nil
}

// Close abandons the unread body and frees its connection for reuse when possible. ReleaseResponse calls it.
func (resp *Response) Close() error {
	var err error
	if resp.body != nil {
		err = resp.body.Close()
		resp.body = nil
	}
	if resp.cancel != nil {
		resp.cancel()
		resp.cancel = nil
	}
	resp.Body = noBody{}
	return err
}

// clientResponse is the protocol-facing side of a Response, kept off its public method set.
type clientResponse Response

var _ exchange.ClientResponse = (*clientResponse)(nil)

func (r *clientResponse) SetStatus(status, major, minor int) {
	r.status = status
	switch {
	case major == 1 && minor == 1:
		r.proto = "HTTP/1.1"
	case major == 1 && minor == 0:
		r.proto = "HTTP/1.0"
	case major == 2:
		r.proto = "HTTP/2.0"
	case major == 3:
		r.proto = "HTTP/3.0"
	}
}

func (r *clientResponse) AddHeader(name, value []byte) error { return r.header.addWire(name, value) }

func (r *clientResponse) AddTrailer(name, value []byte) error { return r.trailer.addWire(name, value) }

func (r *clientResponse) SetBody(body io.ReadCloser, contentLength int64) {
	r.contentLength = contentLength
	r.body = body
	if body == nil {
		r.Body = noBody{}
		return
	}
	r.Body = &r.rbody
}

// responseBody stands between the caller and the protocol's reader, which goes back to its connection at
// EOF and may then serve another request. Reads after that, even through a kept reference, never reach it.
type responseBody struct{ resp *Response }

func (b *responseBody) Read(p []byte) (int, error) {
	r := b.resp
	if r.body == nil {
		if r.bodyErr != nil {
			return 0, r.bodyErr
		}
		return 0, io.EOF
	}
	n, err := r.body.Read(p)
	if err != nil {
		r.body = nil
		if !errors.Is(err, io.EOF) {
			r.bodyErr = err
		}
	}
	return n, err
}
