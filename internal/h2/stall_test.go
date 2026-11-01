package h2

import (
	"errors"
	"io"
	"os"
	"testing"
	"time"

	"github.com/erizeta/qilat/internal/exchange"
	"golang.org/x/net/http2"
)

const testStall = 60 * time.Millisecond

func stallConfig() *Config {
	cfg := testConfig()
	cfg.ReadStallTimeout, cfg.WriteStallTimeout = testStall, testStall
	return cfg
}

// checkStalled verifies the error a handler got after its client stopped making progress.
func checkStalled(t *testing.T, err error, x *fakeExchange) {
	t.Helper()
	if !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Errorf("handler error %v, want one wrapping os.ErrDeadlineExceeded", err)
	}
	if x.ctx.Err() == nil {
		t.Error("request context not canceled")
	}
}

// readResponse reads frames until stream id ends and returns its DATA, failing on RST_STREAM.
func (tc *testConn) readResponse(id uint32, onData func(*http2.DataFrame)) []byte {
	tc.t.Helper()
	var body []byte
	for {
		switch f := tc.read().(type) {
		case *http2.RSTStreamFrame:
			if f.StreamID == id {
				tc.t.Fatalf("stream %d reset: %v", id, f.ErrCode)
			}
		case *http2.HeadersFrame:
			if f.StreamID == id && f.StreamEnded() {
				return body
			}
		case *http2.DataFrame:
			if f.StreamID != id {
				continue
			}
			body = append(body, f.Data()...)
			if f.StreamEnded() {
				return body
			}
			if onData != nil {
				onData(f)
			}
		}
	}
}

func TestReadStallResetsStream(t *testing.T) {
	got := make(chan error, 1)
	var exch *fakeExchange
	tc := startConn(t, func(x *fakeExchange, w exchange.ResponseSink) error {
		exch = x
		_, err := io.ReadAll(x.body)
		got <- err
		return err
	}, stallConfig())
	tc.get(1, false, "content-length", "10")
	tc.write(func() error { return tc.fr.WriteData(1, false, []byte("ab")) }) // and nothing more
	tc.expectRST(1, http2.ErrCodeCancel)
	checkStalled(t, <-got, exch)
}

func TestSlowUploadIsNotAStall(t *testing.T) {
	tc := startConn(t, func(x *fakeExchange, w exchange.ResponseSink) error {
		b, err := io.ReadAll(x.body)
		if err != nil {
			return err
		}
		if err := w.WriteHeader(200, fields{}, int64(len(b))); err != nil {
			return err
		}
		if _, err := w.Write(b); err != nil {
			return err
		}
		return w.Finish()
	}, stallConfig())
	tc.get(1, false)
	for i := range 10 { // several stall timeouts in total, but never one without data
		tc.write(func() error {
			time.Sleep(testStall / 3)
			return tc.fr.WriteData(1, i == 9, []byte{'a' + byte(i)})
		})
	}
	if body := tc.readResponse(1, nil); string(body) != "abcdefghij" {
		t.Fatalf("body %q", body)
	}
}

func TestWriteStallResetsStream(t *testing.T) {
	got := make(chan error, 1)
	var exch *fakeExchange
	tc := startConn(t, func(x *fakeExchange, w exchange.ResponseSink) error {
		if x.target == "/other" {
			return okServe(x, w)
		}
		exch = x
		if err := w.WriteHeader(200, fields{}, 100); err != nil {
			return err
		}
		_, err := w.Write(make([]byte, 100))
		got <- err
		return err
	}, stallConfig())
	// The client grants no send window and never updates it.
	tc.write(func() error { return tc.fr.WriteSettings(http2.Setting{ID: http2.SettingInitialWindowSize, Val: 0}) })
	tc.get(1, true)
	tc.expectRST(1, http2.ErrCodeCancel)
	checkStalled(t, <-got, exch)

	// The connection still serves other streams; their HEADERS need no send window.
	tc.write(func() error {
		return tc.fr.WriteSettings(http2.Setting{ID: http2.SettingInitialWindowSize, Val: 1 << 16})
	})
	tc.write(func() error {
		return tc.fr.WriteHeaders(http2.HeadersFrameParam{StreamID: 3, EndStream: true, EndHeaders: true,
			BlockFragment: tc.headerBlock(":method", "GET", ":scheme", "https", ":path", "/other", ":authority", "example.com")})
	})
	if body := tc.readResponse(3, nil); string(body) != "ok" {
		t.Fatalf("other stream body %q", body)
	}
}

func TestSlowWindowIsNotAStall(t *testing.T) {
	tc := startConn(t, func(x *fakeExchange, w exchange.ResponseSink) error {
		if err := w.WriteHeader(200, fields{}, 100); err != nil {
			return err
		}
		if _, err := w.Write(make([]byte, 100)); err != nil {
			return err
		}
		return w.Finish()
	}, stallConfig())
	tc.write(func() error { return tc.fr.WriteSettings(http2.Setting{ID: http2.SettingInitialWindowSize, Val: 10}) })
	tc.get(1, true)
	// Ten window updates over several stall timeouts, each arriving before one expires.
	body := tc.readResponse(1, func(f *http2.DataFrame) {
		n := uint32(len(f.Data()))
		tc.write(func() error {
			time.Sleep(testStall / 3)
			return tc.fr.WriteWindowUpdate(1, n)
		})
	})
	if len(body) != 100 {
		t.Fatalf("received %d of 100 bytes", len(body))
	}
}
