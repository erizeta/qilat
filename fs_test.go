package qilat

import (
	"bytes"
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/erizeta/qilat/internal/exchange"
)

// makeFileTree creates the files served by the FileServer tests and returns the served root.
// A secret file sits next to the root, so traversal out of it is detectable.
func makeFileTree(t *testing.T) string {
	t.Helper()
	parent := t.TempDir()
	root := filepath.Join(parent, "root")
	big := make([]byte, 100<<10)
	for i := range big {
		big[i] = byte('a' + i%26)
	}
	files := map[string][]byte{
		"../secret.txt":       []byte("secret"),
		"dir/a.txt":           []byte("hello a"),
		"dir/b.html":          []byte("<p>b</p>"),
		"dir/q?#x.txt":        []byte("odd name"),
		"dir/sub/c.txt":       []byte("c"),
		"site/index.html":     []byte("<h1>index</h1>"),
		"noext":               []byte("<!DOCTYPE html><title>sniffed</title>"),
		"binary":              {0, 1, 2, 3, 0xff},
		"empty.txt":           nil,
		"big.bin":             big,
		"a%2fb/escaped.txt":   []byte("escaped dir"),
		"unicode/é ü.txt":     []byte("unicode"),
		"dir/.hidden/x.txt":   []byte("hidden"),
		"site/sub/index.html": []byte("<h1>sub</h1>"),
	}
	mtime := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	for name, data := range files {
		p := filepath.Join(root, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, data, 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Chtimes(p, mtime, mtime); err != nil {
			t.Fatal(err)
		}
	}
	return root
}

type fsResult struct {
	status int
	header string
	body   string
}

// normalizeBoundary replaces the random multipart boundary so two responses can be compared.
func normalizeBoundary(header http.Header, body string) (string, string) {
	ct := header.Get("Content-Type")
	_, boundary, ok := strings.Cut(ct, "boundary=")
	if !ok {
		return ct, body
	}
	return strings.ReplaceAll(ct, boundary, "BOUNDARY"), strings.ReplaceAll(body, boundary, "BOUNDARY")
}

func fetch(t *testing.T, client *http.Client, method, url string, header [][2]string) fsResult {
	t.Helper()
	req, err := http.NewRequest(method, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, h := range header {
		req.Header.Set(h[0], h[1])
	}
	resp, err := client.Do(req) //nolint:bodyclose // readAll closes the body
	body := readAll(t, resp, err)
	ct, body := normalizeBoundary(resp.Header, body)
	var fields []string
	for k, vs := range resp.Header {
		switch k {
		case "Date":
		case "Content-Type":
			fields = append(fields, k+"="+ct)
		default:
			fields = append(fields, k+"="+strings.Join(vs, ","))
		}
	}
	slices.Sort(fields)
	return fsResult{resp.StatusCode, strings.Join(fields, "; "), body}
}

// TestFileServerMatchesNetHTTP serves the same tree with qilat and net/http and compares status, header
// fields (except Date), and body for each request.
func TestFileServerMatchesNetHTTP(t *testing.T) {
	root := makeFileTree(t)
	modtime := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	content := strings.Repeat("0123456789", 100)

	qmux := NewServeMux()
	qmux.Handle("/", FileServer(os.DirFS(root)))
	qmux.Handle("/static/", StripPrefix("/static", FileServer(os.DirFS(root))))
	qmux.HandleFunc("/content", func(w ResponseWriter, r *Request) {
		w.Header().Set("ETag", `"v1"`)
		ServeContent(w, r, "data.txt", modtime, strings.NewReader(content))
	})
	qmux.HandleFunc("/servefile/", func(w ResponseWriter, r *Request) {
		ServeFile(w, r, filepath.Join(root, "dir", "a.txt"))
	})
	smux := http.NewServeMux()
	smux.Handle("/", http.FileServerFS(os.DirFS(root)))
	smux.Handle("/static/", http.StripPrefix("/static", http.FileServerFS(os.DirFS(root))))
	smux.HandleFunc("/content", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("ETag", `"v1"`)
		http.ServeContent(w, r, "data.txt", modtime, strings.NewReader(content))
	})
	smux.HandleFunc("/servefile/", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, filepath.Join(root, "dir", "a.txt"))
	})

	ts := startServer(t, qmux.ServeHTTP)
	std := httptest.NewServer(smux)
	defer std.Close()
	client := &http.Client{
		Transport:     &http.Transport{},
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		Timeout:       5 * time.Second,
	}
	defer client.CloseIdleConnections()

	lastMod := modtime.Format(http.TimeFormat)
	older := modtime.Add(-time.Hour).Format(http.TimeFormat)
	newer := modtime.Add(time.Hour).Format(http.TimeFormat)
	type hdr = [][2]string
	for _, rq := range []struct {
		method, path string
		header       hdr
	}{
		{"GET", "/", nil},
		{"GET", "/dir", nil},
		{"GET", "/dir?x=1", nil},
		{"GET", "/dir/", nil},
		{"GET", "/dir/a.txt", nil},
		{"POST", "/dir/a.txt", nil},
		{"GET", "/dir/a.txt/", nil},
		{"GET", "/dir/b.html", nil},
		{"GET", "/dir/q%3F%23x.txt", nil},
		{"GET", "/dir/sub", nil},
		{"GET", "/dir/.hidden/x.txt", nil},
		{"GET", "/site/", nil},
		{"GET", "/site", nil},
		{"GET", "/site/index.html", nil},
		{"GET", "/site/index.html?a=b", nil},
		{"GET", "/site/sub/", nil},
		{"GET", "/noext", nil},
		{"GET", "/binary", nil},
		{"GET", "/empty.txt", nil},
		{"GET", "/big.bin", nil},
		{"HEAD", "/big.bin", nil},
		{"GET", "/unicode/%C3%A9%20%C3%BC.txt", nil},
		{"GET", "/a%2fb/escaped.txt", nil},
		{"GET", "/a%2fb", nil},
		{"GET", "/missing", nil},
		{"GET", "/dir/a.txt/x", nil},
		// Traversal attempts.
		{"GET", "/../secret.txt", nil},
		{"GET", "/%2e%2e/secret.txt", nil},
		{"GET", "/dir/..%2f..%2fsecret.txt", nil},
		{"GET", "/dir/%2e%2e/%2e%2e/secret.txt", nil},
		{"GET", "/dir/..%5c..%5csecret.txt", nil},
		{"GET", "/dir/%00", nil},
		// Ranges.
		{"GET", "/big.bin", hdr{{"Range", "bytes=0-9"}}},
		{"GET", "/big.bin", hdr{{"Range", "bytes=-10"}}},
		{"GET", "/big.bin", hdr{{"Range", "bytes=102390-"}}},
		{"GET", "/big.bin", hdr{{"Range", "bytes=10-5"}}},
		{"GET", "/big.bin", hdr{{"Range", "bytes=999999-"}}},
		{"GET", "/big.bin", hdr{{"Range", "bytes=0-1,5-6,100-200"}}},
		{"HEAD", "/big.bin", hdr{{"Range", "bytes=0-1,5-6"}}},
		{"GET", "/big.bin", hdr{{"Range", "bytes=0-,0-,0-"}}},
		{"GET", "/big.bin", hdr{{"Range", "units=0-1"}}},
		{"GET", "/big.bin", hdr{{"Range", "bytes=" + strings.Repeat("0-0,", 200) + "0-0"}}},
		{"GET", "/empty.txt", hdr{{"Range", "bytes=5-"}}},
		{"GET", "/dir/a.txt", hdr{{"Range", "bytes=2-4"}}},
		// Conditional requests.
		{"GET", "/dir/a.txt", hdr{{"If-Modified-Since", lastMod}}},
		{"GET", "/dir/a.txt", hdr{{"If-Modified-Since", older}}},
		{"GET", "/dir/a.txt", hdr{{"If-Modified-Since", "garbage"}}},
		{"POST", "/dir/a.txt", hdr{{"If-Modified-Since", lastMod}}},
		{"GET", "/dir/", hdr{{"If-Modified-Since", lastMod}}},
		{"GET", "/dir/a.txt", hdr{{"If-Unmodified-Since", older}}},
		{"GET", "/dir/a.txt", hdr{{"If-Unmodified-Since", newer}}},
		{"GET", "/dir/a.txt", hdr{{"If-None-Match", "*"}}},
		{"POST", "/dir/a.txt", hdr{{"If-None-Match", "*"}}},
		{"GET", "/dir/a.txt", hdr{{"If-None-Match", `"x"`}}},
		{"GET", "/dir/a.txt", hdr{{"If-Match", `"x"`}}},
		{"GET", "/dir/a.txt", hdr{{"If-Match", "*"}}},
		{"GET", "/big.bin", hdr{{"Range", "bytes=0-9"}, {"If-Range", lastMod}}},
		{"GET", "/big.bin", hdr{{"Range", "bytes=0-9"}, {"If-Range", older}}},
		{"GET", "/big.bin", hdr{{"Range", "bytes=0-9"}, {"If-Range", `"etag"`}}},
		// ServeContent with an ETag.
		{"GET", "/content", nil},
		{"GET", "/content", hdr{{"If-None-Match", `W/"v1"`}}},
		{"GET", "/content", hdr{{"If-None-Match", `"a", "v1"`}}},
		{"GET", "/content", hdr{{"If-Match", `"v1"`}}},
		{"GET", "/content", hdr{{"If-Match", `W/"v1"`}}},
		{"GET", "/content", hdr{{"Range", "bytes=0-4"}, {"If-Range", `"v1"`}}},
		{"GET", "/content", hdr{{"Range", "bytes=0-4"}, {"If-Range", `W/"v1"`}}},
		{"GET", "/content", hdr{{"Range", "bytes=0-4,10-14"}}},
		{"GET", "/content", hdr{{"Range", "bytes=2000-"}}},
		// ServeFile and StripPrefix.
		{"GET", "/servefile/x", nil},
		{"GET", "/servefile/../x", nil},
		{"GET", "/servefile/index.html", nil},
		{"GET", "/static/dir/a.txt", nil},
		{"GET", "/static/dir", nil},
		{"GET", "/static/site/index.html", nil},
	} {
		got := fetch(t, client, rq.method, ts.url(rq.path), rq.header)
		want := fetch(t, client, rq.method, std.URL+rq.path, rq.header)
		if got != want {
			t.Errorf("%s %s %v:\n got %d %s\n     %q\nwant %d %s\n     %q", rq.method, rq.path, rq.header,
				got.status, got.header, trim(got.body), want.status, want.header, trim(want.body))
		}
	}
	if _, err := os.Stat(filepath.Join(root, "..", "secret.txt")); err != nil {
		t.Fatal(err) // the traversal requests above must have had something to find
	}
}

func TestFileServerAllProtocols(t *testing.T) {
	root := makeFileTree(t)
	want, err := os.ReadFile(filepath.Join(root, "big.bin"))
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range startAllProtocols(t, FileServer(os.DirFS(root))) {
		resp, err := c.client.Get(c.base + "/big.bin") //nolint:bodyclose // readAll closes the body
		if body := readAll(t, resp, err); body != string(want) || resp.ContentLength != int64(len(want)) {
			t.Errorf("%s: %d bytes, content-length %d", c.name, len(body), resp.ContentLength)
		}
		req, _ := http.NewRequest("GET", c.base+"/big.bin", nil)
		req.Header.Set("Range", "bytes=50000-50009")
		resp, err = c.client.Do(req) //nolint:bodyclose // readAll closes the body
		if body := readAll(t, resp, err); resp.StatusCode != 206 || body != string(want[50000:50010]) {
			t.Errorf("%s range: %d %q", c.name, resp.StatusCode, body)
		}
	}
}

// readFromSink records how the core hands a body to a protocol that supports ReadFrom.
type readFromSink struct {
	fakeSink
	readers []io.Reader
}

func (s *readFromSink) ReadFrom(r io.Reader) (int64, error) {
	s.readers = append(s.readers, r)
	return io.Copy(&s.body, r)
}

var _ exchange.ReaderFromSink = (*readFromSink)(nil)

func TestFileServerUsesReadFrom(t *testing.T) {
	root := makeFileTree(t)
	sink := &readFromSink{}
	x := acquireExchange(&exchangeConfig{handler: FileServer(os.DirFS(root))})
	request{target: "/big.bin"}.fill(t, x)
	if err := x.Serve(sink); err != nil {
		t.Fatal(err)
	}
	x.Release()
	if sink.status != 200 || sink.body.Len() != 100<<10 || sink.cl != 100<<10 {
		t.Fatalf("status %d, %d bytes, cl %d", sink.status, sink.body.Len(), sink.cl)
	}
	// sendfile unwraps one *io.LimitedReader to find the file.
	if len(sink.readers) != 1 {
		t.Fatalf("ReadFrom called %d times", len(sink.readers))
	}
	lr, ok := sink.readers[0].(*io.LimitedReader)
	if !ok {
		t.Fatalf("ReadFrom got %T", sink.readers[0])
	}
	if _, ok := lr.R.(*os.File); !ok {
		t.Fatalf("ReadFrom got a LimitedReader of %T, want *os.File", lr.R)
	}
}

func TestResponseReadFrom(t *testing.T) {
	big := bytes.Repeat([]byte("x"), 3*responseBufferSize)
	for _, tt := range []struct {
		name      string
		declared  int
		src       []byte
		wantErr   error
		readFrom  bool
		wantBytes int
	}{
		{"declared", len(big), big, nil, true, len(big)},
		{"too long", len(big) - 1, big, ErrContentLength, true, len(big) - 1},
		{"undeclared streams through Write", -1, big, nil, false, len(big)},
		{"small is buffered", 10, big[:10], nil, false, 10},
	} {
		t.Run(tt.name, func(t *testing.T) {
			sink := &readFromSink{}
			x := acquireExchange(&exchangeConfig{handler: HandlerFunc(func(w ResponseWriter, r *Request) {
				if tt.declared >= 0 {
					w.Header().Set("Content-Length", strconv.Itoa(tt.declared))
				}
				// Hide WriteTo, or io.Copy would never call ReadFrom.
				_, err := io.Copy(w, readerOnly{bytes.NewReader(tt.src)})
				if err != tt.wantErr { //nolint:errorlint // the exact sentinel is expected
					t.Errorf("copy error %v, want %v", err, tt.wantErr)
				}
			})})
			request{}.fill(t, x)
			_ = x.Serve(sink)
			x.Release()
			if got := len(sink.readers) > 0; got != tt.readFrom {
				t.Errorf("ReadFrom used: %v", got)
			}
			if sink.body.Len() != tt.wantBytes {
				t.Errorf("%d bytes sent, want %d", sink.body.Len(), tt.wantBytes)
			}
		})
	}
}

func TestServeFileRejectsDotDot(t *testing.T) {
	sink, err := serve(t, request{target: "/a/../b"}, func(w ResponseWriter, r *Request) {
		ServeFile(w, r, "/etc/hosts")
	})
	if err != nil || sink.status != 400 {
		t.Fatalf("status %d, err %v", sink.status, err)
	}
}

func TestFileServerNonSeekableFS(t *testing.T) {
	sink, err := serve(t, request{target: "/f.txt", headers: [][2]string{{"range", "bytes=0-1"}}},
		FileServer(noSeekFS{}).ServeHTTP)
	if err != nil || sink.status != 416 {
		t.Fatalf("status %d, err %v, body %q", sink.status, err, sink.body.String())
	}
}

// noSeekFS serves one file whose fs.File cannot seek.
type noSeekFS struct{}

func (noSeekFS) Open(name string) (fs.File, error) {
	if name != "f.txt" {
		return nil, fs.ErrNotExist
	}
	return &noSeekOpenFile{r: strings.NewReader("data")}, nil
}

type noSeekOpenFile struct{ r io.Reader }

func (f *noSeekOpenFile) Read(p []byte) (int, error) { return f.r.Read(p) }
func (f *noSeekOpenFile) Close() error               { return nil }
func (f *noSeekOpenFile) Stat() (fs.FileInfo, error) { return noSeekInfo{}, nil }

type noSeekInfo struct{}

func (noSeekInfo) Name() string       { return "f.txt" }
func (noSeekInfo) Size() int64        { return 4 }
func (noSeekInfo) Mode() fs.FileMode  { return 0o444 }
func (noSeekInfo) ModTime() time.Time { return time.Time{} }
func (noSeekInfo) IsDir() bool        { return false }
func (noSeekInfo) Sys() any           { return nil }
