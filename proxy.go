package qilat

import (
	"bufio"
	"context"
	"encoding/base64"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"sync"
	"time"

	"golang.org/x/net/http/httpproxy"
)

// ProxyFromEnvironment returns the proxy for req from the HTTP_PROXY, HTTPS_PROXY, and NO_PROXY environment
// variables, like [net/http.ProxyFromEnvironment]. Set it as Transport.Proxy. The variables are read once.
func ProxyFromEnvironment(req *Request) (*url.URL, error) {
	envProxyOnce.Do(func() { envProxyFunc = httpproxy.FromEnvironment().ProxyFunc() })
	u := &url.URL{Scheme: string(req.uri.Scheme()), Host: string(req.uri.Host())}
	return envProxyFunc(u)
}

var (
	envProxyOnce sync.Once
	envProxyFunc func(*url.URL) (*url.URL, error)
)

// proxyAuthorization returns the Basic credentials of a proxy URL, or nil.
func proxyAuthorization(proxy *url.URL) []byte {
	if proxy.User == nil {
		return nil
	}
	pass, _ := proxy.User.Password()
	creds := proxy.User.Username() + ":" + pass
	return []byte("Basic " + base64.StdEncoding.EncodeToString([]byte(creds)))
}

// connectTunnel asks an HTTP proxy on nc to open a tunnel to addr (RFC 9110 section 9.3.6).
func connectTunnel(ctx context.Context, nc net.Conn, proxy *url.URL, addr string) error {
	if d, ok := ctx.Deadline(); ok {
		_ = nc.SetDeadline(d)
		defer func() { _ = nc.SetDeadline(time.Time{}) }()
	}
	stop := context.AfterFunc(ctx, func() { _ = nc.SetDeadline(aLongTimeAgo) })
	defer stop()
	req := "CONNECT " + addr + " HTTP/1.1\r\nHost: " + addr + "\r\n"
	if auth := proxyAuthorization(proxy); auth != nil {
		req += "Proxy-Authorization: " + string(auth) + "\r\n"
	}
	if _, err := nc.Write([]byte(req + "\r\n")); err != nil {
		return err
	}
	// The proxy sends nothing after its response until the TLS handshake starts, so this reader cannot
	// swallow tunnel bytes.
	resp, err := http.ReadResponse(bufio.NewReaderSize(nc, 1<<10), nil)
	if err != nil {
		if ctx.Err() != nil {
			return context.Cause(ctx)
		}
		return fmt.Errorf("qilat: proxy CONNECT: %w", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != 200 {
		return fmt.Errorf("qilat: proxy CONNECT to %s: %s", addr, resp.Status)
	}
	return nil
}

var aLongTimeAgo = time.Unix(1, 0)
