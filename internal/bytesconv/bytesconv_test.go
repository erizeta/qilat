package bytesconv

import (
	"math"
	"strconv"
	"testing"
)

func TestB2SAndS2B(t *testing.T) {
	b := []byte("hello")
	if got := B2S(b); got != "hello" {
		t.Fatalf("B2S = %q", got)
	}
	if got := B2S(nil); got != "" {
		t.Fatalf("B2S(nil) = %q", got)
	}
	if got := string(S2B("world")); got != "world" {
		t.Fatalf("S2B = %q", got)
	}
	if got := S2B(""); len(got) != 0 {
		t.Fatalf("S2B(\"\") len = %d", len(got))
	}
}

func TestToLower(t *testing.T) {
	tests := []struct{ in, want string }{
		{"", ""},
		{"Content-Type", "content-type"},
		{"X-ABC-123_z", "x-abc-123_z"},
		{"already", "already"},
		{"\xc3\x84", "\xc3\x84"}, // non-ASCII bytes are untouched
	}
	for _, tt := range tests {
		if got := string(ToLower([]byte(tt.in))); got != tt.want {
			t.Errorf("ToLower(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}

func TestEqualFoldLower(t *testing.T) {
	tests := []struct {
		lower, s string
		want     bool
	}{
		{"content-type", "Content-Type", true},
		{"content-type", "CONTENT-TYPE", true},
		{"content-type", "content-type", true},
		{"content-type", "content-typ", false},
		{"content-type", "content-tipe", false},
		{"", "", true},
		{"a", "B", false},
	}
	for _, tt := range tests {
		if got := EqualFoldLower([]byte(tt.lower), tt.s); got != tt.want {
			t.Errorf("EqualFoldLower(%q, %q) = %v, want %v", tt.lower, tt.s, got, tt.want)
		}
	}
}

func TestParseUint(t *testing.T) {
	tests := []struct {
		in     string
		want   int64
		wantOK bool
	}{
		{"0", 0, true},
		{"42", 42, true},
		{"007", 7, true},
		{strconv.FormatInt(math.MaxInt64, 10), math.MaxInt64, true},
		{"9223372036854775808", 0, false}, // MaxInt64 + 1
		{"99999999999999999999", 0, false},
		{"", 0, false},
		{"-1", 0, false},
		{"+1", 0, false},
		{" 1", 0, false},
		{"1 ", 0, false},
		{"1a", 0, false},
		{"0x10", 0, false},
	}
	for _, tt := range tests {
		got, ok := ParseUint([]byte(tt.in))
		if got != tt.want || ok != tt.wantOK {
			t.Errorf("ParseUint(%q) = %d, %v; want %d, %v", tt.in, got, ok, tt.want, tt.wantOK)
		}
	}
}

func TestUnhex(t *testing.T) {
	for c := 0; c < 256; c++ {
		got, ok := Unhex(byte(c))
		want, err := strconv.ParseUint(string(rune(c)), 16, 8)
		wantOK := err == nil && c < 128
		if ok != wantOK || (ok && uint64(got) != want) {
			t.Errorf("Unhex(%#x) = %d, %v; want %d, %v", c, got, ok, want, wantOK)
		}
	}
}

func TestAllocs(t *testing.T) {
	b := []byte("Content-Length")
	n := testing.AllocsPerRun(100, func() {
		_ = B2S(b)
		_ = S2B("x")
		ToLower(b)
		_ = EqualFoldLower(b, "Content-Length")
		_, _ = ParseUint([]byte("12345"))
	})
	if n != 0 {
		t.Fatalf("allocs = %v, want 0", n)
	}
}

func FuzzParseUint(f *testing.F) {
	for _, s := range []string{"0", "123", "9223372036854775807", "9223372036854775808", "-1", "a"} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, s string) {
		got, ok := ParseUint([]byte(s))
		allDigits := len(s) > 0
		for i := 0; i < len(s); i++ {
			if s[i] < '0' || s[i] > '9' {
				allDigits = false
			}
		}
		want, err := strconv.ParseInt(s, 10, 64)
		wantOK := allDigits && err == nil
		if ok != wantOK || (ok && got != want) {
			t.Fatalf("ParseUint(%q) = %d, %v; strconv = %d, %v", s, got, ok, want, err)
		}
	})
}
