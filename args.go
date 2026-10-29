package qilat

import (
	"bytes"
	"iter"

	"github.com/erizeta/qilat/internal/bytesconv"
)

// Args holds decoded query or form arguments in order. Keys are case-sensitive.
// Returned byte slices are valid until the next Parse or until the handler returns.
type Args struct {
	pairs []field
}

// Parse replaces the arguments with the decoded pairs of an application/x-www-form-urlencoded string.
// Like [net/url.ParseQuery], pairs with invalid escapes or a ';' are skipped and the rest are kept.
func (a *Args) Parse(query []byte) {
	a.pairs = a.pairs[:0]
	for len(query) > 0 {
		var pair []byte
		pair, query, _ = bytes.Cut(query, []byte("&"))
		if len(pair) == 0 || bytes.IndexByte(pair, ';') >= 0 || !validEscapes(pair) {
			continue
		}
		key, value, _ := bytes.Cut(pair, []byte("="))
		n := len(a.pairs)
		if n < cap(a.pairs) {
			a.pairs = a.pairs[:n+1]
		} else {
			a.pairs = append(a.pairs, field{})
		}
		p := &a.pairs[n]
		p.name = appendUnescape(p.name[:0], key, true)
		p.value = appendUnescape(p.value[:0], value, true)
	}
}

// Len returns the number of arguments, counting repeated keys separately.
func (a *Args) Len() int { return len(a.pairs) }

// Has reports whether key is present.
func (a *Args) Has(key string) bool { return a.index(key) >= 0 }

// Peek returns the first value for key, or nil if there is none.
func (a *Args) Peek(key string) []byte {
	if i := a.index(key); i >= 0 {
		return a.pairs[i].value
	}
	return nil
}

// Get returns a copy of the first value for key, or "" if there is none. It allocates.
func (a *Args) Get(key string) string { return string(a.Peek(key)) }

// PeekAll returns every value for key in order. It allocates the returned slice.
func (a *Args) PeekAll(key string) [][]byte {
	var vs [][]byte
	for _, p := range a.pairs {
		if bytesconv.B2S(p.name) == key {
			vs = append(vs, p.value)
		}
	}
	return vs
}

// All returns an iterator over every key and value in order.
func (a *Args) All() iter.Seq2[[]byte, []byte] {
	return func(yield func(key, value []byte) bool) {
		for _, p := range a.pairs {
			if !yield(p.name, p.value) {
				return
			}
		}
	}
}

// Reset removes all arguments while keeping allocated memory for reuse.
func (a *Args) Reset() { a.pairs = a.pairs[:0] }

func (a *Args) index(key string) int {
	for i := range a.pairs {
		if bytesconv.B2S(a.pairs[i].name) == key {
			return i
		}
	}
	return -1
}
