package qilat

import (
	"bytes"
	"compress/flate"
	"compress/gzip"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func deflateEncoder() Encoder {
	return Encoder{Name: "deflate", New: func(w io.Writer) CompressWriter {
		fw, _ := flate.NewWriter(w, flate.DefaultCompression)
		return fw
	}}
}

func TestCompressNegotiate(t *testing.T) {
	comp := &compressor{codings: []*coding{newCoding("deflate", nil), newCoding("gzip", nil)}}
	for _, tt := range []struct{ ae, want string }{
		{"", ""},
		{"gzip", "gzip"},
		{"GZIP", "gzip"},
		{"x-gzip", "gzip"},
		{"gzip;q=0", ""},
		{"gzip; q=0.000", ""},
		{"gzip;q=abc", ""},
		{"gzip;q=2", ""},
		{"identity", ""},
		{"br", ""},
		{"*", "deflate"},
		{"*;q=0, gzip", "gzip"},
		{"*;q=0.1, gzip;q=0.5", "gzip"},
		{"deflate, gzip", "deflate"},
		{"gzip, deflate", "deflate"},
		{"gzip;q=1, deflate;q=0.9", "gzip"},
		{"deflate;q=0.5, gzip;Q=0.8", "gzip"},
		{"br;q=1.0, gzip;q=0.8, *;q=0.1", "gzip"},
		{" gzip ; q=0.5 , deflate ; q=0.4 ", "gzip"},
		{"gzipx, xgzip", ""},
	} {
		got := ""
		if cd := comp.negotiate([]byte(tt.ae)); cd != nil {
			got = cd.name
		}
		if got != tt.want {
			t.Errorf("Accept-Encoding %q: got %q, want %q", tt.ae, got, tt.want)
		}
	}
}

func TestCompressibleType(t *testing.T) {
	for _, tt := range []struct {
		ct   string
		want bool
	}{
		{"", true},
		{"text/html; charset=utf-8", true},
		{"TEXT/Plain", true},
		{"application/json", true},
		{"application/problem+json", true},
		{"application/atom+xml", true},
		{"image/svg+xml", true},
		{"text/event-stream", false},
		{"image/png", false},
		{"application/octet-stream", false},
		{"application/zip", false},
		{"video/mp4", false},
	} {
		if got := compressibleType([]byte(tt.ct)); got != tt.want {
			t.Errorf("%q: %v", tt.ct, got)
		}
	}
}

func TestCompressInvalidConfigPanics(t *testing.T) {
	for _, cfg := range []CompressConfig{
		{Level: 42},
		{Encoders: []Encoder{{Name: "bad name", New: deflateEncoder().New}}},
		{Encoders: []Encoder{{Name: "zstd"}}},
	} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("%+v did not panic", cfg)
				}
			}()
			CompressWith(cfg)
		}()
	}
}

func decode(t *testing.T, encoding string, body []byte) string {
	t.Helper()
	var r io.Reader
	switch encoding {
	case "":
		return string(body)
	case "gzip":
		zr, err := gzip.NewReader(bytes.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		r = zr
	case "deflate":
		r = flate.NewReader(bytes.NewReader(body))
	default:
		t.Fatalf("unknown encoding %q", encoding)
	}
	b, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func TestCompress(t *testing.T) {
	root := makeFileTree(t)
	text := strings.Repeat("compressible text ", 500)
	if err := os.WriteFile(filepath.Join(root, "big.txt"), []byte(text), 0o600); err != nil {
		t.Fatal(err)
	}
	mux := NewServeMux()
	mux.HandleFunc("/text", func(w ResponseWriter, r *Request) {
		w.Header().Set("Content-Type", "text/plain")
		w.Header().Set("ETag", `"v1"`)
		for range 10 { // small writes, so the start is buffered before the decision
			_, _ = w.WriteString(text[:len(text)/10])
		}
	})
	mux.HandleFunc("/small", func(w ResponseWriter, r *Request) {
		w.Header().Set("Content-Type", "text/plain")
		_, _ = w.WriteString("tiny")
	})
	mux.HandleFunc("/declared", func(w ResponseWriter, r *Request) {
		w.Header().Set("Content-Length", "5000")
		_, _ = w.WriteString(strings.Repeat("d", 5000))
	})
	mux.HandleFunc("/png", func(w ResponseWriter, r *Request) {
		w.Header().Set("Content-Type", "image/png")
		_, _ = w.WriteString(text)
	})
	mux.HandleFunc("/encoded", func(w ResponseWriter, r *Request) {
		w.Header().Set("Content-Encoding", "br")
		_, _ = w.WriteString(text)
	})
	mux.HandleFunc("/notransform", func(w ResponseWriter, r *Request) {
		w.Header().Set("Cache-Control", "public, no-transform")
		_, _ = w.WriteString(text)
	})
	mux.HandleFunc("/nocontent", func(w ResponseWriter, r *Request) { w.WriteHeader(204) })
	mux.HandleFunc("/trailer", func(w ResponseWriter, r *Request) {
		_, _ = w.WriteString(text)
		w.(TrailerSetter).Trailer().Set("X-Sum", "1")
	})
	mux.Handle("/std", FromStd(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "<html>"+text)
	})))
	mux.Handle("/files/", StripPrefix("/files", FileServer(os.DirFS(root))))
	h := CompressWith(CompressConfig{Encoders: []Encoder{deflateEncoder()}})(mux)

	type want struct {
		encoding, vary, etag string
		body                 string
	}
	compressed := func(enc string) want { return want{enc, "Accept-Encoding", "", text} }
	for _, c := range startAllProtocols(t, h) {
		for _, tt := range []struct {
			method, path, ae, rng string
			want                  want
		}{
			{"GET", "/text", "gzip", "", want{"gzip", "Accept-Encoding", `W/"v1"`, text}},
			{"GET", "/text", "deflate, gzip", "", want{"deflate", "Accept-Encoding", `W/"v1"`, text}},
			// Without the header, Go clients would ask for gzip and decompress transparently.
			{"GET", "/text", "identity", "", want{"", "Accept-Encoding", `"v1"`, text}},
			{"GET", "/text", "br", "", want{"", "Accept-Encoding", `"v1"`, text}},
			{"GET", "/small", "gzip", "", want{"", "Accept-Encoding", "", "tiny"}},
			{"GET", "/declared", "gzip", "", want{"gzip", "Accept-Encoding", "", strings.Repeat("d", 5000)}},
			{"GET", "/png", "gzip", "", want{"", "", "", text}},
			{"GET", "/encoded", "gzip", "", want{"br", "", "", text}},
			{"GET", "/notransform", "gzip", "", want{"", "", "", text}},
			{"GET", "/nocontent", "gzip", "", want{"", "", "", ""}},
			{"GET", "/trailer", "gzip", "", compressed("gzip")},
			{"GET", "/std", "gzip", "", want{"gzip", "Accept-Encoding", "", "<html>" + text}},
			{"GET", "/files/big.txt", "gzip", "", compressed("gzip")},
			{"GET", "/files/big.txt", "gzip", "bytes=0-9", want{"", "", "", text[:10]}},
			{"GET", "/files/big.bin", "gzip", "", want{"", "", "", ""}},
		} {
			req, _ := http.NewRequest(tt.method, c.base+tt.path, nil)
			if tt.ae != "" {
				req.Header.Set("Accept-Encoding", tt.ae)
			}
			if tt.rng != "" {
				req.Header.Set("Range", tt.rng)
			}
			resp, err := c.client.Do(req) //nolint:bodyclose // readAll closes the body
			raw := readAll(t, resp, err)
			got := want{
				resp.Header.Get("Content-Encoding"), strings.Join(resp.Header["Vary"], ","), resp.Header.Get("ETag"), "",
			}
			if tt.path == "/files/big.bin" {
				got.body = "" // only the headers matter: binary content is not compressed
			} else if got.encoding == "br" {
				got.body = raw
			} else {
				got.body = decode(t, got.encoding, []byte(raw))
			}
			if got != tt.want {
				t.Errorf("%s %s (Accept-Encoding %q, Range %q):\n got %s vary=%q etag=%q %q\nwant %s vary=%q etag=%q %q",
					c.name, tt.path, tt.ae, tt.rng, got.encoding, got.vary, got.etag, trim(got.body),
					tt.want.encoding, tt.want.vary, tt.want.etag, trim(tt.want.body))
			}
			if got.encoding != "" && got.encoding != "br" && resp.ContentLength >= 0 && resp.ContentLength != int64(len(raw)) {
				t.Errorf("%s %s: Content-Length %d for %d bytes", c.name, tt.path, resp.ContentLength, len(raw))
			}
			if tt.path == "/trailer" && resp.Trailer.Get("X-Sum") != "1" {
				t.Errorf("%s: trailer %v", c.name, resp.Trailer)
			}
		}
	}
}

// TestCompressStreaming checks that flushed data reaches the client before the handler returns, both for
// server-sent events (sent as they are) and for compressed streams.
func TestCompressStreaming(t *testing.T) {
	var mu sync.Mutex
	release := make(chan struct{})
	released := func() chan struct{} {
		mu.Lock()
		defer mu.Unlock()
		return release
	}
	reset := func() {
		mu.Lock()
		release = make(chan struct{})
		mu.Unlock()
	}
	mux := NewServeMux()
	mux.HandleFunc("/events", func(w ResponseWriter, r *Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.WriteString("data: one\n\n")
		_ = w.Flush()
		<-released()
		_, _ = w.WriteString("data: two\n\n")
	})
	mux.HandleFunc("/ndjson", func(w ResponseWriter, r *Request) {
		w.Header().Set("Content-Type", "application/x-ndjson")
		_, _ = w.WriteString(`{"n":1}` + "\n")
		_ = w.Flush()
		<-released()
		_, _ = w.WriteString(`{"n":2}` + "\n")
	})
	h := Compress(gzip.BestSpeed)(mux)
	for _, c := range startAllProtocols(t, h) {
		reset()
		req, _ := http.NewRequest("GET", c.base+"/events", nil)
		req.Header.Set("Accept-Encoding", "gzip")
		resp, err := c.client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		buf := make([]byte, len("data: one\n\n"))
		if _, err := io.ReadFull(resp.Body, buf); err != nil || string(buf) != "data: one\n\n" ||
			resp.Header.Get("Content-Encoding") != "" {
			t.Errorf("%s events: %q %v %v", c.name, buf, err, resp.Header)
		}
		close(released())
		rest, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if string(rest) != "data: two\n\n" {
			t.Errorf("%s events rest: %q", c.name, rest)
		}

		reset()
		req, _ = http.NewRequest("GET", c.base+"/ndjson", nil)
		req.Header.Set("Accept-Encoding", "gzip")
		resp, err = c.client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		if resp.Header.Get("Content-Encoding") != "gzip" {
			t.Errorf("%s ndjson: %v", c.name, resp.Header)
		}
		zr, err := gzip.NewReader(resp.Body)
		if err != nil {
			t.Fatal(err)
		}
		line := make([]byte, len(`{"n":1}`+"\n"))
		done := make(chan error, 1)
		go func() { _, err := io.ReadFull(zr, line); done <- err }()
		select {
		case err := <-done:
			if err != nil || string(line) != `{"n":1}`+"\n" {
				t.Errorf("%s ndjson first line: %q %v", c.name, line, err)
			}
		case <-time.After(5 * time.Second):
			t.Errorf("%s: flushed compressed data did not arrive", c.name)
		}
		close(released())
		rest, _ = io.ReadAll(zr)
		_ = resp.Body.Close()
		if string(rest) != `{"n":2}`+"\n" {
			t.Errorf("%s ndjson rest: %q", c.name, rest)
		}
	}
}

func TestCompressHEADMatchesGET(t *testing.T) {
	text := strings.Repeat("head ", 1000)
	h := Compress(0)(HandlerFunc(func(w ResponseWriter, r *Request) { _, _ = w.WriteString(text) }))
	headers := map[string][]string{}
	for _, method := range []string{"GET", "HEAD"} {
		sink, err := serve(t, request{method: method, headers: [][2]string{{"accept-encoding", "gzip"}}}, h.ServeHTTP)
		if err != nil {
			t.Fatal(err)
		}
		headers[method] = sink.header
	}
	if strings.Join(headers["GET"], ";") != strings.Join(headers["HEAD"], ";") {
		t.Fatalf("GET %v\nHEAD %v", headers["GET"], headers["HEAD"])
	}
}

func BenchmarkCompress(b *testing.B) {
	small := []byte("Hello, World!")
	large := bytes.Repeat([]byte("compressible text "), 200)
	for _, bc := range []struct {
		name string
		body []byte
		ae   string
	}{
		{"small", small, "gzip"},
		{"large-identity", large, ""},
		{"large-gzip", large, "gzip"},
	} {
		b.Run(bc.name, func(b *testing.B) {
			h := Compress(gzip.BestSpeed)(HandlerFunc(func(w ResponseWriter, r *Request) {
				w.Header().Set("Content-Type", "text/plain")
				_, _ = w.Write(bc.body)
			}))
			cfg := &exchangeConfig{handler: h}
			ae, method, target, aeName := []byte(bc.ae), []byte("GET"), []byte("/"), []byte("accept-encoding")
			b.ReportAllocs()
			for b.Loop() {
				x := acquireExchange(cfg)
				_ = x.SetMethod(method)
				_ = x.SetTarget(target)
				if len(ae) > 0 {
					_ = x.AddHeader(aeName, ae)
				}
				_ = x.Serve(discardSink{})
				x.Release()
			}
		})
	}
}
