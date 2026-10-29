package qilat

import (
	"net/url"
	"reflect"
	"slices"
	"testing"
)

func TestArgsParse(t *testing.T) {
	tests := []struct{ query, want string }{
		{"", ""},
		{"a=1&b=2", "a=1,b=2"},
		{"a=1&a=2", "a=1,a=2"},
		{"a", "a="},
		{"=v", "=v"},
		{"a=b=c", "a=b=c"},
		{"&&a=1&&", "a=1"},
		{"k=a+b%20c%2B", "k=a b c+"},
		{"Key=v&key=w", "Key=v,key=w"},
		{"bad=%zz&ok=1", "ok=1"},
		{"semi=1;x=2&ok=1", "ok=1"},
		{"%41=%42", "A=B"},
	}
	for _, tt := range tests {
		var a Args
		a.Parse([]byte(tt.query))
		if got := argsString(&a); got != tt.want {
			t.Errorf("Parse(%q) = %q, want %q", tt.query, got, tt.want)
		}
	}
}

func TestArgsAccessors(t *testing.T) {
	var a Args
	a.Parse([]byte("x=1&y=2&x=3&Y=4"))
	if a.Len() != 4 || !a.Has("x") || a.Has("z") {
		t.Fatalf("Len %d Has(x) %v Has(z) %v", a.Len(), a.Has("x"), a.Has("z"))
	}
	if a.Get("x") != "1" || string(a.Peek("y")) != "2" || a.Peek("z") != nil || a.Get("z") != "" {
		t.Fatal("lookup mismatch")
	}
	if got := toStrings(a.PeekAll("x")); !slices.Equal(got, []string{"1", "3"}) {
		t.Fatalf("PeekAll = %q", got)
	}
	n := 0
	for range a.All() {
		n++
		break
	}
	if n != 1 {
		t.Fatal("All did not stop after break")
	}
	a.Reset()
	if a.Len() != 0 {
		t.Fatal("Reset left pairs")
	}
	a.Parse([]byte("q=reused"))
	if a.Get("q") != "reused" {
		t.Fatal("reuse after Reset failed")
	}
}

func TestArgsAllocs(t *testing.T) {
	if raceEnabled {
		t.Skip("allocation counts are not meaningful with the race detector")
	}
	var a Args
	q := []byte("name=qilat&page=2&tag=a+b&tag=c%20d&empty=")
	run := func() {
		a.Parse(q)
		_ = a.Peek("tag")
		_ = a.Has("page")
	}
	run()
	if n := testing.AllocsPerRun(100, run); n != 0 {
		t.Fatalf("allocs = %v, want 0", n)
	}
}

// FuzzArgs checks that Args decodes the same keys and values as net/url.ParseQuery.
func FuzzArgs(f *testing.F) {
	for _, s := range []string{"a=1&b=2", "a+b=c%20d", "bad=%zz&ok=1", "x;y=1", "=", "&&"} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, query string) {
		var a Args
		a.Parse([]byte(query))
		got := url.Values{}
		for k, v := range a.All() {
			got[string(k)] = append(got[string(k)], string(v))
		}
		want, _ := url.ParseQuery(query)
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("Parse(%q) = %v, net/url = %v", query, got, want)
		}
	})
}
