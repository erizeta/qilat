package benchmarks

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
)

// h1Conn is a minimal HTTP/1.1 client so every server is measured against the same, cheap client.
// It only understands Content-Length responses, which all benchmark handlers produce.
type h1Conn struct {
	dial func() (net.Conn, error)
	conn net.Conn
	br   *bufio.Reader
	body []byte
}

func dialH1(dial func() (net.Conn, error)) (*h1Conn, error) {
	c := &h1Conn{dial: dial, br: bufio.NewReaderSize(nil, 16<<10)}
	return c, c.redial()
}

func (c *h1Conn) redial() error {
	conn, err := c.dial()
	if err != nil {
		return err
	}
	c.conn = conn
	c.br.Reset(conn)
	return nil
}

// Close resets the connection (SO_LINGER=0) so new-connection benchmarks do not pile up TIME_WAIT sockets.
func (c *h1Conn) Close() error {
	if c.conn == nil {
		return nil
	}
	if tc, ok := c.conn.(*net.TCPConn); ok {
		_ = tc.SetLinger(0)
	}
	err := c.conn.Close()
	c.conn = nil
	return err
}

// do sends a pre-rendered request and returns the response body, valid until the next call.
func (c *h1Conn) do(req []byte) ([]byte, error) {
	if c.conn == nil {
		if err := c.redial(); err != nil {
			return nil, err
		}
	}
	if _, err := c.conn.Write(req); err != nil {
		return nil, err
	}
	return c.readResponse()
}

// doPipelined sends n pipelined requests in a single write, then reads n responses and checks each body length.
func (c *h1Conn) doPipelined(batch []byte, n, wantLen int) error {
	if c.conn == nil {
		if err := c.redial(); err != nil {
			return err
		}
	}
	if _, err := c.conn.Write(batch); err != nil {
		return err
	}
	for range n {
		body, err := c.readResponse()
		if err != nil {
			return err
		}
		if len(body) != wantLen {
			return fmt.Errorf("body length = %d, want %d", len(body), wantLen)
		}
	}
	return nil
}

func (c *h1Conn) readResponse() ([]byte, error) {
	line, err := c.br.ReadSlice('\n')
	if err != nil {
		return nil, fmt.Errorf("read status line: %w", err)
	}
	if !bytes.HasPrefix(line, []byte("HTTP/1.1 200 ")) {
		return nil, fmt.Errorf("unexpected status line %q", bytes.TrimSpace(line))
	}
	contentLength := -1
	for {
		line, err = c.br.ReadSlice('\n')
		if err != nil {
			return nil, fmt.Errorf("read header: %w", err)
		}
		line = bytes.TrimRight(line, "\r\n")
		if len(line) == 0 {
			break
		}
		name, value, ok := bytes.Cut(line, []byte(":"))
		if ok && bytes.EqualFold(name, []byte("Content-Length")) {
			contentLength, err = strconv.Atoi(string(bytes.TrimSpace(value)))
			if err != nil {
				return nil, fmt.Errorf("bad Content-Length %q", value)
			}
		}
	}
	if contentLength < 0 {
		return nil, errors.New("response without Content-Length")
	}
	if cap(c.body) < contentLength {
		c.body = make([]byte, contentLength)
	}
	c.body = c.body[:contentLength]
	if _, err := io.ReadFull(c.br, c.body); err != nil {
		return nil, fmt.Errorf("read body: %w", err)
	}
	return c.body, nil
}
