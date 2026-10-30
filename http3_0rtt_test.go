package qilat

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"io"
	"log/slog"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/quic-go/qpack"
	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/quicvarint"
)

// holdHandshake wraps the server's UDP socket and delays datagrams carrying a client Handshake packet, which
// holds the client's TLS Finished. Requests sent in 0-RTT are then processed before the handshake completes,
// as they would be for a replayed connection, whose handshake never completes.
type holdHandshake struct {
	net.PacketConn
	on    atomic.Bool
	delay time.Duration
}

func (c *holdHandshake) ReadFrom(p []byte) (int, net.Addr, error) {
	n, addr, err := c.PacketConn.ReadFrom(p)
	if err == nil && c.on.Load() && hasLongHeaderType(p[:n], 2) {
		time.Sleep(c.delay)
	}
	return n, addr, err
}

// hasLongHeaderType reports whether a datagram holds a QUIC long-header packet of type typ (RFC 9000
// section 17.2); packets may be coalesced.
func hasLongHeaderType(b []byte, typ byte) bool {
	for len(b) > 6 && b[0]&0x80 != 0 {
		t := b[0] >> 4 & 3
		if t == typ {
			return true
		}
		i := 5
		i += 1 + int(b[i])
		if i >= len(b) {
			return false
		}
		i += 1 + int(b[i])
		if t == 0 { // Initial packets carry a token
			if i >= len(b) {
				return false
			}
			tl, n, err := quicvarint.Parse(b[i:])
			if err != nil {
				return false
			}
			i += n + int(tl)
		}
		if i >= len(b) {
			return false
		}
		l, n, err := quicvarint.Parse(b[i:])
		if err != nil {
			return false
		}
		if i += n + int(l); i > len(b) {
			return false
		}
		b = b[i:]
	}
	return false
}

// rawH3Request sends one request on a QUIC connection and returns the response status.
func rawH3Request(t *testing.T, conn *quic.Conn, method string) string {
	t.Helper()
	str, err := conn.OpenStreamSync(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var fields bytes.Buffer
	enc := qpack.NewEncoder(&fields)
	for _, f := range [][2]string{{":method", method}, {":scheme", "https"}, {":path", "/"}, {":authority", "localhost"}} {
		_ = enc.WriteField(qpack.HeaderField{Name: f[0], Value: f[1]})
	}
	frame := quicvarint.Append(quicvarint.Append(nil, 0x1), uint64(fields.Len()))
	if _, err := str.Write(append(frame, fields.Bytes()...)); err != nil {
		t.Fatal(err)
	}
	_ = str.Close()
	br := bufio.NewReader(str)
	typ, err := quicvarint.Read(br)
	if err != nil {
		t.Fatal(err)
	}
	length, err := quicvarint.Read(br)
	if err != nil || typ != 0x1 {
		t.Fatalf("first frame type %d: %v", typ, err)
	}
	p := make([]byte, length)
	if _, err := io.ReadFull(br, p); err != nil {
		t.Fatal(err)
	}
	next := qpack.NewDecoder().Decode(p)
	for {
		f, err := next()
		if err != nil {
			t.Fatalf("no :status: %v", err)
		}
		if f.Name == ":status" {
			return f.Value
		}
	}
}

func TestHTTP3ZeroRTTRejectsUnsafeRequests(t *testing.T) {
	checkGoroutines(t)
	cert, err := GenerateDevCert()
	if err != nil {
		t.Fatal(err)
	}
	srv := &Server{
		Handler:   HandlerFunc(func(w ResponseWriter, r *Request) { _, _ = w.WriteString("ok") }),
		TLSConfig: &tls.Config{Certificates: []tls.Certificate{cert}},
		HTTP3:     HTTP3Config{Allow0RTT: true},
		ErrorLog:  slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	udp, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	pc := &holdHandshake{PacketConn: udp, delay: 300 * time.Millisecond}
	done := make(chan error, 1)
	go func() { done <- srv.ServeQUIC(pc) }()
	defer func() {
		_ = srv.Shutdown(context.Background())
		if err := <-done; !errors.Is(err, ErrServerClosed) {
			t.Errorf("ServeQUIC = %v", err)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	tlsConf := &tls.Config{InsecureSkipVerify: true, NextProtos: []string{"h3"}, //nolint:gosec // self-signed test certificate
		ClientSessionCache: tls.NewLRUClientSessionCache(4)}
	addr := udp.LocalAddr().String()

	// A first connection gets a session ticket, which enables 0-RTT on the next ones.
	first, err := quic.DialAddr(ctx, addr, tlsConf, &quic.Config{})
	if err != nil {
		t.Fatal(err)
	}
	if got := rawH3Request(t, first, "POST"); got != "200" {
		t.Fatalf("1-RTT POST: %s", got)
	}
	_ = first.CloseWithError(0, "")

	pc.on.Store(true)
	for _, tt := range []struct{ method, want string }{{"POST", "425"}, {"GET", "200"}} {
		conn, err := quic.DialAddrEarly(ctx, addr, tlsConf, &quic.Config{})
		if err != nil {
			t.Fatal(err)
		}
		got := rawH3Request(t, conn, tt.method)
		<-conn.HandshakeComplete()
		if !conn.ConnectionState().Used0RTT {
			t.Fatal("the connection did not use 0-RTT")
		}
		if got != tt.want {
			t.Errorf("0-RTT %s: status %s, want %s", tt.method, got, tt.want)
		}
		_ = conn.CloseWithError(0, "")
	}

	// Once the handshake completes, the same request is no longer early and is served.
	pc.on.Store(false)
	conn, err := quic.DialAddrEarly(ctx, addr, tlsConf, &quic.Config{})
	if err != nil {
		t.Fatal(err)
	}
	<-conn.HandshakeComplete()
	if got := rawH3Request(t, conn, "POST"); got != "200" {
		t.Errorf("POST after handshake: %s", got)
	}
	_ = conn.CloseWithError(0, "")
}
