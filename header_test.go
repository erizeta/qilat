package qilat

import (
	"slices"
	"strings"
	"testing"
)

func TestHeaderNormalizesNames(t *testing.T) {
	var h Header
	h.Set("Content-Type", "text/plain")
	for _, name := range []string{"content-type", "Content-Type", "CONTENT-TYPE"} {
		if got := string(h.Peek(name)); got != "text/plain" {
			t.Errorf("Peek(%q) = %q", name, got)
		}
		if !h.Has(name) {
			t.Errorf("Has(%q) = false", name)
		}
	}
	for name := range h.All() {
		if string(name) != "content-type" {
			t.Errorf("stored name = %q, want lowercase", name)
		}
	}
	if h.Peek("content-length") != nil || h.Has("content-length") || h.Get("content-length") != "" {
		t.Error("missing field reported as present")
	}
}

func TestHeaderDuplicatesAndOrder(t *testing.T) {
	var h Header
	h.Add("Accept", "a")
	h.Add("X-One", "1")
	h.Add("accept", "b")
	h.Add("X-Two", "2")
	h.Add("ACCEPT", "c")

	if got := toStrings(h.PeekAll("Accept")); !slices.Equal(got, []string{"a", "b", "c"}) {
		t.Fatalf("PeekAll = %q", got)
	}
	if h.Len() != 5 || h.Get("accept") != "a" {
		t.Fatalf("Len = %d, Get = %q", h.Len(), h.Get("accept"))
	}

	h.Set("Accept", "z")
	if got := fields(&h); !slices.Equal(got, []string{"accept=z", "x-one=1", "x-two=2"}) {
		t.Fatalf("after Set = %q", got)
	}

	h.Add("X-Three", "3")
	h.Del("x-one")
	if got := fields(&h); !slices.Equal(got, []string{"accept=z", "x-two=2", "x-three=3"}) {
		t.Fatalf("after Del = %q", got)
	}
	h.Del("missing")
	if h.Len() != 3 {
		t.Fatalf("Del of missing name changed Len to %d", h.Len())
	}
	if h.PeekAll("missing") != nil {
		t.Fatal("PeekAll of missing name is not nil")
	}
}

func TestHeaderSetAppendsWhenMissing(t *testing.T) {
	var h Header
	h.Set("X-A", "1")
	h.SetBytes("X-B", []byte("2"))
	if got := fields(&h); !slices.Equal(got, []string{"x-a=1", "x-b=2"}) {
		t.Fatalf("fields = %q", got)
	}
}

func TestHeaderInvalidNamePanics(t *testing.T) {
	for _, name := range []string{"", "a b", "a:b", "a\r", "a\n", "x\x00", "caf\xc3\xa9", "(x)", "a/b"} {
		for op, f := range map[string]func(h *Header){
			"Set":      func(h *Header) { h.Set(name, "v") },
			"SetBytes": func(h *Header) { h.SetBytes(name, []byte("v")) },
			"Add":      func(h *Header) { h.Add(name, "v") },
			"AddBytes": func(h *Header) { h.AddBytes(name, []byte("v")) },
		} {
			func() {
				defer func() {
					if recover() == nil {
						t.Errorf("%s(%q) did not panic", op, name)
					}
				}()
				f(new(Header))
			}()
		}
	}
	var h Header
	h.Set("!#$%&'*+-.^_`|~09AZaz", "ok") // every token character class
	if h.Len() != 1 {
		t.Fatal("valid token rejected")
	}
}

func TestHeaderSanitizesValues(t *testing.T) {
	var h Header
	h.Set("X-Injected", "a\r\nSet-Cookie: x\x00")
	if got := h.Get("x-injected"); got != "a  Set-Cookie: x " {
		t.Fatalf("value = %q", got)
	}
	src := []byte("keep\r\n")
	h.AddBytes("X-Copy", src)
	if string(src) != "keep\r\n" {
		t.Fatalf("caller's slice modified: %q", src)
	}
	src[0] = 'K'
	if h.Get("x-copy") != "keep  " {
		t.Fatalf("value aliases caller's slice: %q", h.Get("x-copy"))
	}
}

func TestHeaderAddWire(t *testing.T) {
	var h Header
	if err := h.addWire([]byte("Host"), []byte("example.com")); err != nil {
		t.Fatal(err)
	}
	if h.Get("host") != "example.com" {
		t.Fatalf("Get = %q", h.Get("host"))
	}
	bad := []struct{ name, value string }{
		{"", "v"}, {"a b", "v"}, {"x", "a\rb"}, {"x", "a\nb"}, {"x", "a\x00b"},
	}
	for _, tt := range bad {
		if err := h.addWire([]byte(tt.name), []byte(tt.value)); err == nil {
			t.Errorf("addWire(%q, %q) accepted", tt.name, tt.value)
		}
	}
	if h.Len() != 1 {
		t.Fatalf("rejected fields were stored: Len = %d", h.Len())
	}
}

func TestHeaderResetCopyToAll(t *testing.T) {
	var h, dst Header
	h.Add("A", "1")
	h.Add("B", "2")
	dst.Add("Old", "x")
	h.CopyTo(&dst)
	h.Set("A", "changed")
	if got := fields(&dst); !slices.Equal(got, []string{"a=1", "b=2"}) {
		t.Fatalf("CopyTo = %q", got)
	}

	n := 0
	for range h.All() {
		n++
		break
	}
	if n != 1 {
		t.Fatal("All did not stop after break")
	}

	h.Reset()
	if h.Len() != 0 || h.Has("a") {
		t.Fatal("Reset left fields")
	}
	h.Add("C", "3")
	if got := fields(&h); !slices.Equal(got, []string{"c=3"}) {
		t.Fatalf("after reuse = %q", got)
	}
}

func TestHeaderList(t *testing.T) {
	var h Header
	h.Add("A", "1")
	h.Add("B", "2")
	l := (*headerList)(&h)
	if l.Len() != 2 {
		t.Fatalf("Len = %d", l.Len())
	}
	if name, value := l.At(1); string(name) != "b" || string(value) != "2" {
		t.Fatalf("At(1) = %q, %q", name, value)
	}
}

func TestHeaderDeadPanics(t *testing.T) {
	h := Header{dead: true}
	defer func() {
		if recover() == nil {
			t.Fatal("dead header did not panic")
		}
	}()
	h.Peek("x")
}

func TestHeaderAllocs(t *testing.T) {
	if raceEnabled {
		t.Skip("allocation counts are not meaningful with the race detector")
	}
	var h Header
	value := []byte("application/json")
	run := func() {
		h.Reset()
		for range 20 {
			h.Add("X-Bench-Header", "value")
		}
		h.Set("Content-Type", "text/plain")
		h.SetBytes("Content-Type", value)
		h.AddBytes("Accept", value)
		_ = h.Peek("accept")
		_ = h.Has("content-type")
		h.Del("x-bench-header")
	}
	run()
	if n := testing.AllocsPerRun(100, run); n != 0 {
		t.Fatalf("allocs = %v, want 0", n)
	}
}

func FuzzHeader(f *testing.F) {
	f.Add("Content-Type", "text/plain")
	f.Add("x", "a\r\nb")
	f.Add("bad name", "v")
	f.Fuzz(func(t *testing.T, name, value string) {
		var h Header
		if !validName(name) {
			defer func() {
				if recover() == nil {
					t.Fatalf("Set(%q) did not panic", name)
				}
			}()
			h.Set(name, value)
			return
		}
		h.Set(name, value)
		want := strings.NewReplacer("\r", " ", "\n", " ", "\x00", " ").Replace(value)
		if got := h.Get(strings.ToUpper(name)); got != want {
			t.Fatalf("Get = %q, want %q", got, want)
		}
		h.Add(name, value)
		if len(h.PeekAll(name)) != 2 {
			t.Fatalf("PeekAll len = %d", len(h.PeekAll(name)))
		}
		if err := h.addWire([]byte(name), []byte(value)); (err == nil) != validValue([]byte(value)) {
			t.Fatalf("addWire(%q) err = %v", value, err)
		}
		h.Del(name)
		if h.Has(name) || h.Len() != 0 {
			t.Fatalf("Del left %d fields", h.Len())
		}
	})
}

func toStrings(bs [][]byte) []string {
	out := make([]string, len(bs))
	for i, b := range bs {
		out[i] = string(b)
	}
	return out
}

func fields(h *Header) []string {
	var out []string
	for name, value := range h.All() {
		out = append(out, string(name)+"="+string(value))
	}
	return out
}
