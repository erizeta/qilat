package qilat

import (
	"errors"
	"net/url"
	"strings"
	"testing"
)

func TestURIParse(t *testing.T) {
	tests := []struct {
		target                           string
		form                             targetForm
		scheme, host                     string
		rawPath, path, rawQuery, queryKV string
	}{
		{target: "/", form: formOrigin, rawPath: "/", path: "/"},
		{target: "/a/b?x=1&y=2", form: formOrigin, rawPath: "/a/b", path: "/a/b", rawQuery: "x=1&y=2", queryKV: "x=1,y=2"},
		{target: "/a%20b", form: formOrigin, rawPath: "/a%20b", path: "/a b"},
		{target: "/a%2Fb", form: formOrigin, rawPath: "/a%2Fb", path: "/a/b"},
		{target: "/a+b?q=c+d", form: formOrigin, rawPath: "/a+b", path: "/a+b", rawQuery: "q=c+d", queryKV: "q=c d"},
		{target: "/?", form: formOrigin, rawPath: "/", path: "/"},
		{target: "//double", form: formOrigin, rawPath: "//double", path: "//double"},
		{target: "/caf\xc3\xa9", form: formOrigin, rawPath: "/caf\xc3\xa9", path: "/caf\xc3\xa9"},
		{target: "*", form: formAsterisk, rawPath: "*", path: "*"},
		{target: "http://example.com/x?q", form: formAbsolute, scheme: "http", host: "example.com", rawPath: "/x", path: "/x", rawQuery: "q", queryKV: "q="},
		{target: "https://example.com", form: formAbsolute, scheme: "https", host: "example.com", rawPath: "/", path: "/"},
		{target: "http://example.com?q=1", form: formAbsolute, scheme: "http", host: "example.com", rawPath: "/", path: "/", rawQuery: "q=1", queryKV: "q=1"},
		{target: "h2c+x.y-z://[::1]:8080/p", form: formAbsolute, scheme: "h2c+x.y-z", host: "[::1]:8080", rawPath: "/p", path: "/p"},
		{target: "example.com:443", form: formAuthority, host: "example.com:443"},
	}
	for _, tt := range tests {
		t.Run(tt.target, func(t *testing.T) {
			var u URI
			if err := u.Parse([]byte(tt.target)); err != nil {
				t.Fatalf("Parse: %v", err)
			}
			check := func(what, got, want string) {
				t.Helper()
				if got != want {
					t.Errorf("%s = %q, want %q", what, got, want)
				}
			}
			if u.form != tt.form {
				t.Errorf("form = %d, want %d", u.form, tt.form)
			}
			check("Raw", string(u.Raw()), tt.target)
			check("String", u.String(), tt.target)
			check("Scheme", string(u.Scheme()), tt.scheme)
			check("Host", string(u.Host()), tt.host)
			check("RawPath", string(u.RawPath()), tt.rawPath)
			check("Path", string(u.Path()), tt.path)
			check("RawQuery", string(u.RawQuery()), tt.rawQuery)
			check("Query", argsString(u.Query()), tt.queryKV)
		})
	}
}

func TestURIParseErrors(t *testing.T) {
	for _, target := range []string{
		"", "/a b", "/a\tb", "/a#frag", "/\x7f", "/\x00",
		"/a%zz", "/a%2", "/a%", "/a%g0",
		"http://", "http:///path", "http://user@example.com/", "http://example.com/%zz",
		"example.com/x", "example.com?x", "user@example.com", "1http://x/",
	} {
		var u URI
		if err := u.Parse([]byte(target)); !errors.Is(err, ErrInvalidURI) {
			t.Errorf("Parse(%q) error = %v, want ErrInvalidURI", target, err)
		}
	}
}

func TestURIReuse(t *testing.T) {
	var u URI
	steps := []struct{ target, path, query string }{
		{"/plain?a=1", "/plain", "a=1"},
		{"/a%20b?b=2", "/a b", "b=2"},
		{"/plain2", "/plain2", ""},
		{"/c%41t", "/cAt", ""},
	}
	for _, s := range steps {
		if err := u.Parse([]byte(s.target)); err != nil {
			t.Fatal(err)
		}
		// Raw must survive path decoding; an earlier version decoded into memory that aliased raw.
		if got := string(u.Path()); got != s.path {
			t.Fatalf("Parse(%q).Path = %q, want %q", s.target, got, s.path)
		}
		if got := string(u.Raw()); got != s.target {
			t.Fatalf("Raw after Path = %q, want %q", got, s.target)
		}
		if got := argsString(u.Query()); got != s.query {
			t.Fatalf("Parse(%q).Query = %q, want %q", s.target, got, s.query)
		}
	}
}

func TestURIZeroAndCopy(t *testing.T) {
	var zero URI
	if string(zero.RawPath()) != "/" || string(zero.Path()) != "/" || zero.String() != "/" {
		t.Fatalf("zero URI: RawPath %q Path %q String %q", zero.RawPath(), zero.Path(), zero.String())
	}
	var src, dst URI
	if err := src.Parse([]byte("/x%20y?k=v")); err != nil {
		t.Fatal(err)
	}
	src.CopyTo(&dst)
	if err := src.Parse([]byte("/other")); err != nil {
		t.Fatal(err)
	}
	if string(dst.Path()) != "/x y" || dst.Query().Get("k") != "v" {
		t.Fatalf("copy = %q %q", dst.Path(), dst.Query().Get("k"))
	}
	zero.CopyTo(&dst)
	if dst.String() != "/" || len(dst.Raw()) != 0 {
		t.Fatalf("copy of zero URI = %q", dst.String())
	}
}

func TestURIAllocs(t *testing.T) {
	if raceEnabled {
		t.Skip("allocation counts are not meaningful with the race detector")
	}
	var u URI
	target := []byte("/api/v1/users/42%20x?name=qilat&page=2&tag=a+b")
	run := func() {
		if err := u.Parse(target); err != nil {
			t.Fatal(err)
		}
		_ = u.Path()
		_ = u.Query().Peek("page")
	}
	run()
	if n := testing.AllocsPerRun(100, run); n != 0 {
		t.Fatalf("allocs = %v, want 0", n)
	}
}

// FuzzURI checks origin-form parsing against net/url, which qilat must agree with whenever both accept a target.
func FuzzURI(f *testing.F) {
	for _, s := range []string{"/", "/a/b?x=1", "/a%20b", "/a%zz", "*", "http://h/p?q", "h:1", "/a#b", "//x"} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, target string) {
		var u URI
		err := u.Parse([]byte(target))
		if err != nil {
			return
		}
		_ = u.Query()
		if !strings.HasPrefix(target, "/") {
			return
		}
		std, stdErr := url.ParseRequestURI(target)
		if stdErr != nil {
			t.Fatalf("qilat accepted %q, net/url rejected it: %v", target, stdErr)
		}
		if got := string(u.Path()); got != std.Path {
			t.Fatalf("Path(%q) = %q, net/url = %q", target, got, std.Path)
		}
		if got := string(u.RawQuery()); got != std.RawQuery {
			t.Fatalf("RawQuery(%q) = %q, net/url = %q", target, got, std.RawQuery)
		}
	})
}

func argsString(a *Args) string {
	var b strings.Builder
	for k, v := range a.All() {
		if b.Len() > 0 {
			b.WriteByte(',')
		}
		b.WriteString(string(k) + "=" + string(v))
	}
	return b.String()
}
