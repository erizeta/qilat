package exchange

import (
	"errors"
	"io"
)

// ClientRequest is an outgoing request, filled by the core and sent by a protocol's RoundTrip.
// The protocol must not keep it, or anything it points to, after RoundTrip returns.
type ClientRequest struct {
	Method string
	Scheme string // "http" or "https"
	// Authority is host[:port], sent as Host (HTTP/1.1) or :authority (HTTP/2, HTTP/3).
	Authority []byte
	// Target is the request-target: origin form, or absolute form when sent to an HTTP proxy.
	Target []byte
	Header HeaderList
	// ProxyAuthorization, when set, is sent as Proxy-Authorization to an HTTP proxy.
	ProxyAuthorization []byte
	// Body is nil when the request has none. ContentLength is -1 when unknown.
	Body          io.Reader
	ContentLength int64
}

// ClientResponse receives a response from a protocol. The core implements it.
type ClientResponse interface {
	SetStatus(status, major, minor int)
	AddHeader(name, value []byte) error
	AddTrailer(name, value []byte) error
	// SetBody hands over the response body, which the core closes when it is done with it. Closing before
	// EOF abandons the rest of the body. contentLength is -1 when unknown.
	SetBody(body io.ReadCloser, contentLength int64)
}

// ErrNotProcessed wraps errors for requests the server did not act on, such as a stale keep-alive connection
// closing before any response byte or an HTTP/2 stream refused by GOAWAY. Idempotent requests may be retried.
var ErrNotProcessed = errors.New("qilat: request not processed")

// NotProcessed marks err as [ErrNotProcessed] while keeping it in the error chain.
func NotProcessed(err error) error { return notProcessed{err} }

type notProcessed struct{ err error }

func (e notProcessed) Error() string   { return e.err.Error() }
func (e notProcessed) Unwrap() []error { return []error{ErrNotProcessed, e.err} }
