// Package exchange is the contract between protocol implementations and the qilat core.
// Protocols cannot import the root package, so they fill requests and write responses through these interfaces.
package exchange

import (
	"bufio"
	"crypto/tls"
	"errors"
	"io"
	"net"
)

// ErrBodyTooLarge is returned by request body readers when the body exceeds the server limit.
var ErrBodyTooLarge = errors.New("qilat: request body too large")

// Exchange is one request and its response, implemented by the core.
// A protocol fills the request, calls Serve once, and calls Release when it no longer uses the exchange.
type Exchange interface {
	SetMethod(method []byte) error
	SetTarget(target []byte) error
	// SetHost sets the Host header (HTTP/1.1) or :authority (HTTP/2, HTTP/3). It is not added to the header list.
	SetHost(host []byte) error
	SetProto(major, minor int)
	AddHeader(name, value []byte) error
	// SetBody sets the request body; contentLength is -1 when unknown. A nil body means no body.
	SetBody(body io.Reader, contentLength int64)
	SetConn(remoteAddr net.Addr, tlsState *tls.ConnectionState)
	// SetDisconnectWatcher registers w, which is told once when the handler starts waiting on the request context.
	SetDisconnectWatcher(w DisconnectWatcher)

	// Serve runs the handler and writes the response to w. A non-nil error means the response is
	// incomplete or broken, so the protocol must not reuse the connection or stream.
	Serve(w ResponseSink) error
	// Cancel cancels the request context, for example when the client disconnects.
	// It may be called concurrently with Serve.
	Cancel()
	Release()
}

// HeaderList is a read-only view of header fields. Names are lowercase.
type HeaderList interface {
	Len() int
	At(i int) (name, value []byte)
}

// ResponseSink puts a response on the wire, implemented by a protocol.
type ResponseSink interface {
	// WriteHeader sends the status and header fields. contentLength is the exact body length, or -1 when
	// the body is streamed. The protocol must not send a body for HEAD requests or 1xx, 204, and 304 responses.
	WriteHeader(status int, header HeaderList, contentLength int64) error
	// Write sends body bytes. It must not retain p.
	Write(p []byte) (int, error)
	Flush() error
	// Finish ends the response.
	Finish() error
}

// DisconnectWatcher lets a protocol detect client disconnects only for handlers that wait on the request
// context, since detection costs a goroutine on HTTP/1.1. WatchDisconnect must not block.
type DisconnectWatcher interface {
	WatchDisconnect()
}

// InformationalSink is implemented by sinks that can send 1xx responses before the final response.
type InformationalSink interface {
	WriteInformational(status int, header HeaderList) error
}

// Hijacker is implemented by sinks that can hand the underlying connection to the handler.
type Hijacker interface {
	Hijack() (net.Conn, *bufio.ReadWriter, error)
}
