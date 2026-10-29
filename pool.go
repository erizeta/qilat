package qilat

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"log/slog"
	"net"
	"runtime/debug"
	"sync"

	"github.com/erizeta/qilat/internal/exchange"
)

// exchangeConfig is shared by every exchange of one server.
type exchangeConfig struct {
	handler Handler
	baseCtx context.Context
	// poison makes released requests and writers panic on use instead of returning them to the pool.
	poison bool
	logger *slog.Logger
}

// serverExchange is the core side of one request/response pair, driven by a protocol.
type serverExchange struct {
	req  Request
	resp response
	cfg  *exchangeConfig
}

var _ exchange.Exchange = (*serverExchange)(nil)

var exchangePool = sync.Pool{New: func() any { return new(serverExchange) }}

func acquireExchange(cfg *exchangeConfig) *serverExchange {
	x := exchangePool.Get().(*serverExchange)
	x.cfg = cfg
	x.req.init(cfg.baseCtx)
	return x
}

func (x *serverExchange) SetMethod(method []byte) error { return x.req.setMethod(method) }

func (x *serverExchange) SetTarget(target []byte) error { return x.req.uri.Parse(target) }

func (x *serverExchange) SetHost(host []byte) error { return x.req.setHost(host) }

func (x *serverExchange) SetProto(major, minor int) { x.req.setProto(major, minor) }

func (x *serverExchange) AddHeader(name, value []byte) error {
	return x.req.header.addWire(name, value)
}

func (x *serverExchange) SetBody(body io.Reader, contentLength int64) {
	if body == nil {
		body = noBody{}
	}
	x.req.Body = body
	x.req.contentLength = contentLength
}

func (x *serverExchange) SetConn(remoteAddr net.Addr, tlsState *tls.ConnectionState) {
	x.req.remoteAddr = remoteAddr
	x.req.tls = tlsState
}

func (x *serverExchange) SetDisconnectWatcher(w exchange.DisconnectWatcher) { x.req.st.watcher = w }

var errHandlerPanic = errors.New("qilat: handler panicked")

func (x *serverExchange) Serve(w exchange.ResponseSink) (err error) {
	x.resp.init(w, &x.req)
	// Like net/http, the request context is canceled once the handler returns.
	defer x.req.st.cancel()
	defer func() {
		if v := recover(); v != nil {
			if v != ErrAbortHandler && x.cfg.logger != nil { //nolint:errorlint // panic values are compared, not wrapped
				x.cfg.logger.Error("qilat: panic serving request",
					"method", x.req.method, "uri", x.req.uri.String(), "panic", v, "stack", string(debug.Stack()))
			}
			err = errHandlerPanic
		}
	}()
	x.cfg.handler.ServeHTTP(&x.resp, &x.req)
	return x.resp.finish()
}

func (x *serverExchange) Cancel() { x.req.st.cancel() }

func (x *serverExchange) Release() {
	x.req.st.cancel()
	if x.cfg.poison {
		x.req.poison()
		x.resp.poison()
		return
	}
	x.req.reset()
	x.resp.reset()
	x.cfg = nil
	exchangePool.Put(x)
}
