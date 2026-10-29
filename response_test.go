package qilat

import (
	"bytes"
	"errors"
	"strings"
	"testing"
)

func TestResponseLargeBodyStreams(t *testing.T) {
	big := bytes.Repeat([]byte("x"), responseBufferSize+1)
	sink, err := serve(t, request{}, func(w ResponseWriter, r *Request) {
		_, _ = w.Write(big[:10])
		_, _ = w.Write(big[10:])
	})
	if err != nil {
		t.Fatal(err)
	}
	if sink.cl != -1 || sink.body.Len() != len(big) {
		t.Fatalf("cl %d body %d", sink.cl, sink.body.Len())
	}
	for _, f := range sink.header {
		if strings.HasPrefix(f, "content-length=") {
			t.Fatalf("streamed response has %q", f)
		}
	}
}

func TestResponseDeclaredContentLength(t *testing.T) {
	sink, err := serve(t, request{}, func(w ResponseWriter, r *Request) {
		w.Header().Set("Content-Length", "6")
		if _, err := w.Write([]byte("abc")); err != nil {
			t.Error(err)
		}
		if _, err := w.Write([]byte("defg")); !errors.Is(err, ErrContentLength) {
			t.Errorf("overflowing Write error = %v", err)
		}
		_, _ = w.Write([]byte("def"))
	})
	if err != nil {
		t.Fatal(err)
	}
	if sink.cl != 6 || sink.body.String() != "abcdef" || !equalStrings(sink.header, []string{"content-length=6"}) {
		t.Fatalf("cl %d body %q header %q", sink.cl, sink.body.String(), sink.header)
	}
}

func TestResponseShortBodyFails(t *testing.T) {
	_, err := serve(t, request{}, func(w ResponseWriter, r *Request) {
		w.Header().Set("Content-Length", "10")
		_, _ = w.Write([]byte("abc"))
	})
	if !errors.Is(err, ErrContentLength) {
		t.Fatalf("Serve error = %v, want ErrContentLength", err)
	}
}

func TestResponseInvalidContentLengthDropped(t *testing.T) {
	sink, err := serve(t, request{}, func(w ResponseWriter, r *Request) {
		w.Header().Set("Content-Length", "-1")
		_, _ = w.Write([]byte("ok"))
	})
	if err != nil {
		t.Fatal(err)
	}
	if sink.cl != 2 || !equalStrings(sink.header, []string{"content-length=2"}) {
		t.Fatalf("cl %d header %q", sink.cl, sink.header)
	}
}

func TestResponseHEAD(t *testing.T) {
	sink, err := serve(t, request{method: "HEAD"}, func(w ResponseWriter, r *Request) {
		n, err := w.Write([]byte("hidden"))
		if n != 6 || err != nil {
			t.Errorf("Write = %d, %v", n, err)
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	if sink.cl != 6 || sink.body.Len() != 0 || !equalStrings(sink.header, []string{"content-length=6"}) {
		t.Fatalf("cl %d body %q header %q", sink.cl, sink.body.String(), sink.header)
	}
}

func TestResponseHEADWithDeclaredLength(t *testing.T) {
	sink, err := serve(t, request{method: "HEAD"}, func(w ResponseWriter, r *Request) {
		w.Header().Set("Content-Length", "100")
	})
	if err != nil {
		t.Fatal(err)
	}
	if sink.cl != 100 {
		t.Fatalf("cl %d", sink.cl)
	}
}

func TestResponseNoBodyStatus(t *testing.T) {
	for _, code := range []int{204, 304} {
		sink, err := serve(t, request{}, func(w ResponseWriter, r *Request) {
			w.WriteHeader(code)
			if _, err := w.Write([]byte("x")); !errors.Is(err, ErrBodyNotAllowed) {
				t.Errorf("%d: Write error = %v", code, err)
			}
		})
		if err != nil {
			t.Fatal(err)
		}
		if sink.status != code || len(sink.header) != 0 || sink.body.Len() != 0 {
			t.Fatalf("%d: status %d header %q body %q", code, sink.status, sink.header, sink.body.String())
		}
	}
}

func TestResponseWriteHeader(t *testing.T) {
	sink, err := serve(t, request{}, func(w ResponseWriter, r *Request) {
		w.WriteHeader(103) // informational codes are not final
		w.WriteHeader(404)
		w.WriteHeader(500) // superfluous
		_, _ = w.WriteString("nf")
	})
	if err != nil {
		t.Fatal(err)
	}
	if sink.status != 404 || sink.body.String() != "nf" {
		t.Fatalf("status %d body %q", sink.status, sink.body.String())
	}
	for _, code := range []int{0, 99, 1000, -1} {
		_, _ = serve(t, request{}, func(w ResponseWriter, r *Request) {
			defer func() {
				if recover() == nil {
					t.Errorf("WriteHeader(%d) did not panic", code)
				}
			}()
			w.WriteHeader(code)
		})
	}
}

func TestResponseFlush(t *testing.T) {
	sink, err := serve(t, request{}, func(w ResponseWriter, r *Request) {
		_, _ = w.Write([]byte("a"))
		if err := w.Flush(); err != nil {
			t.Error(err)
		}
		_, _ = w.Write([]byte("b"))
		if err := w.Flush(); err != nil {
			t.Error(err)
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	if sink.cl != -1 || sink.body.String() != "ab" || sink.flushes != 2 || sink.headerCalls != 1 {
		t.Fatalf("cl %d body %q flushes %d headerCalls %d", sink.cl, sink.body.String(), sink.flushes, sink.headerCalls)
	}
}

func TestResponseFlushBeforeWrite(t *testing.T) {
	sink, err := serve(t, request{}, func(w ResponseWriter, r *Request) {
		_ = w.Flush()
	})
	if err != nil {
		t.Fatal(err)
	}
	if sink.status != 200 || sink.cl != -1 || sink.flushes != 1 {
		t.Fatalf("status %d cl %d flushes %d", sink.status, sink.cl, sink.flushes)
	}
}

func TestResponseSinkErrorsAreSticky(t *testing.T) {
	boom := errors.New("boom")
	big := bytes.Repeat([]byte("x"), responseBufferSize+1)
	tests := []struct {
		name    string
		sink    *fakeSink
		handler HandlerFunc
	}{
		{"header", &fakeSink{failHeader: boom}, func(w ResponseWriter, r *Request) {
			if _, err := w.Write(big); !errors.Is(err, boom) {
				t.Errorf("Write error = %v", err)
			}
			if _, err := w.Write([]byte("x")); !errors.Is(err, boom) {
				t.Errorf("second Write error = %v", err)
			}
		}},
		{"buffered write", &fakeSink{failWrite: boom}, func(w ResponseWriter, r *Request) {
			_, _ = w.Write([]byte("small"))
			if err := w.Flush(); !errors.Is(err, boom) {
				t.Errorf("Flush error = %v", err)
			}
		}},
		{"direct write", &fakeSink{failWrite: boom}, func(w ResponseWriter, r *Request) {
			_ = w.Flush()
			if _, err := w.Write([]byte("x")); !errors.Is(err, boom) {
				t.Errorf("Write error = %v", err)
			}
		}},
		{"flush", &fakeSink{failFlush: boom}, func(w ResponseWriter, r *Request) {
			if err := w.Flush(); !errors.Is(err, boom) {
				t.Errorf("Flush error = %v", err)
			}
			if err := w.Flush(); !errors.Is(err, boom) {
				t.Errorf("second Flush error = %v", err)
			}
		}},
		{"finish header", &fakeSink{failHeader: boom}, func(ResponseWriter, *Request) {}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			x := acquireExchange(&exchangeConfig{handler: tt.handler})
			request{}.fill(t, x)
			if err := x.Serve(tt.sink); !errors.Is(err, boom) {
				t.Fatalf("Serve error = %v, want boom", err)
			}
			x.Release()
		})
	}
}

func TestResponseContentLengthSetAfterWriteHeader(t *testing.T) {
	sink, err := serve(t, request{}, func(w ResponseWriter, r *Request) {
		w.WriteHeader(200)
		w.Header().Set("Content-Length", "999")
		_, _ = w.Write([]byte("abc"))
	})
	if err != nil {
		t.Fatal(err)
	}
	if sink.cl != 3 || !equalStrings(sink.header, []string{"content-length=3"}) {
		t.Fatalf("cl %d header %q", sink.cl, sink.header)
	}
}

func TestResponseContentLengthDroppedWhenStreaming(t *testing.T) {
	sink, err := serve(t, request{}, func(w ResponseWriter, r *Request) {
		w.WriteHeader(200)
		w.Header().Set("Content-Length", "999")
		_ = w.Flush()
	})
	if err != nil {
		t.Fatal(err)
	}
	if sink.cl != -1 || len(sink.header) != 0 {
		t.Fatalf("cl %d header %q", sink.cl, sink.header)
	}
}
