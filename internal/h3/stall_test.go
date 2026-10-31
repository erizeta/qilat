package h3

import (
	"context"
	"errors"
	"io"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/erizeta/qilat/internal/exchange"
	"github.com/quic-go/quic-go"
)

const testStall = 100 * time.Millisecond

func stallConfig(serve func(*fakeExchange, exchange.ResponseSink) error) *Config {
	cfg := testConfig(serve)
	cfg.ReadHeaderTimeout, cfg.ReadStallTimeout, cfg.WriteStallTimeout = testStall, testStall, testStall
	return cfg
}

// stallResult is what a handler saw when its client stopped making progress.
type stallResult struct {
	err      error
	canceled bool
}

func (r stallResult) check(t *testing.T) {
	t.Helper()
	if !errors.Is(r.err, os.ErrDeadlineExceeded) {
		t.Errorf("handler error %v, want one wrapping os.ErrDeadlineExceeded", r.err)
	}
	if !r.canceled {
		t.Error("request context not canceled")
	}
}

func openStream(t *testing.T, conn *quic.Conn, raw []byte) *quic.Stream {
	t.Helper()
	str, err := conn.OpenStreamSync(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	_ = str.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err := str.Write(raw); err != nil {
		t.Fatal(err)
	}
	return str
}

func TestHeaderTimeoutResetsStream(t *testing.T) {
	conn := startServer(t, stallConfig(okServe))
	// A HEADERS frame that announces 100 bytes and carries 1; the client then sends nothing more. (A stream that
	// sends no bytes at all never reaches the server.)
	str := openStream(t, conn, append(appendFrameHeader(nil, frameHeaders, 100), 0))
	start := time.Now()
	_, err := io.ReadAll(str)
	if got := streamErrorCode(err); got != errRequestIncomplete {
		t.Fatalf("stream error %v (%v), want %#x", got, err, uint64(errRequestIncomplete))
	}
	if d := time.Since(start); d > 20*testStall {
		t.Errorf("reset after %v with a %v header timeout", d, testStall)
	}
}

func TestReadStall(t *testing.T) {
	got := make(chan stallResult, 1)
	conn := startServer(t, stallConfig(func(x *fakeExchange, w exchange.ResponseSink) error {
		_, err := io.ReadAll(x.body)
		got <- stallResult{err, x.ctx.Err() != nil}
		return err
	}))
	head := frame(frameHeaders, encodeFields(":method", "POST", ":scheme", "https", ":path", "/", "content-length", "10"))
	// A DATA frame that announces 10 bytes and carries 2; the client then sends nothing more.
	str := openStream(t, conn, append(appendFrameHeader(head, frameData, 10), "ab"...))
	defer str.CancelWrite(0)
	(<-got).check(t)
}

func TestSlowUploadIsNotAStall(t *testing.T) {
	conn := startServer(t, stallConfig(func(x *fakeExchange, w exchange.ResponseSink) error {
		b, err := io.ReadAll(x.body)
		if err != nil {
			return err
		}
		n := []byte(strconv.Itoa(len(b)))
		return w.(exchange.CompleteSink).WriteComplete(200, fields{}, int64(len(n)), n, nil)
	}))
	head := frame(frameHeaders, encodeFields(":method", "POST", ":scheme", "https", ":path", "/"))
	str := openStream(t, conn, appendFrameHeader(head, frameData, 10))
	for range 10 { // several stall timeouts in total, but never one without data
		time.Sleep(testStall / 3)
		if _, err := str.Write([]byte("x")); err != nil {
			t.Fatal(err)
		}
	}
	_ = str.Close()
	resp, err := io.ReadAll(str)
	if err != nil || len(resp) < 2 || string(resp[len(resp)-2:]) != "10" {
		t.Fatalf("response %q, %v", resp, err)
	}
}

func TestWriteStall(t *testing.T) {
	got := make(chan stallResult, 1)
	conn := startServer(t, stallConfig(func(x *fakeExchange, w exchange.ResponseSink) error {
		if err := w.WriteHeader(200, fields{}, -1); err != nil {
			return err
		}
		chunk := make([]byte, 64<<10)
		var err error
		for i := 0; i < 1024 && err == nil; i++ {
			_, err = w.Write(chunk)
		}
		got <- stallResult{err, x.ctx.Err() != nil}
		return err
	}))
	str := openStream(t, conn, frame(frameHeaders, encodeFields(getFields...)))
	_ = str.Close()
	defer str.CancelRead(0) // and never read the response
	(<-got).check(t)
}

func TestSlowReaderIsNotAStall(t *testing.T) {
	const size = 1 << 20
	conn := startServer(t, stallConfig(func(x *fakeExchange, w exchange.ResponseSink) error {
		if err := w.WriteHeader(200, fields{}, size); err != nil {
			return err
		}
		if _, err := w.Write(make([]byte, size)); err != nil { // one Write, far larger than a stall chunk
			return err
		}
		return w.Finish()
	}))
	str := openStream(t, conn, frame(frameHeaders, encodeFields(getFields...)))
	_ = str.Close()
	// 16 KiB every 10 ms takes about 0.65 s, several stall timeouts, but each chunk is taken in time.
	var buf [16 << 10]byte
	n := 0
	for {
		time.Sleep(10 * time.Millisecond)
		m, err := str.Read(buf[:])
		n += m
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("after %d bytes: %v", n, err)
		}
	}
	if n < size {
		t.Fatalf("received %d bytes, want over %d", n, size)
	}
}
