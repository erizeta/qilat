package qilat

import (
	"fmt"
	"iter"

	"github.com/erizeta/qilat/internal/bytesconv"
)

// Header holds HTTP header fields in arrival order with lowercase names and case-insensitive lookups.
// Returned byte slices are valid only until the header changes or the handler returns.
type Header struct {
	fields []field
	// dead is set when the owning request has been released in debug-poison mode.
	dead bool
}

type field struct {
	name  []byte
	value []byte
}

// Len returns the number of header fields, counting repeated names separately.
func (h *Header) Len() int {
	h.live()
	return len(h.fields)
}

// Has reports whether a field with the given name exists.
func (h *Header) Has(name string) bool {
	return h.index(name) >= 0
}

// Peek returns the value of the first field with the given name, or nil if there is none.
// The result must not be modified and is valid only until the header changes or the handler returns.
func (h *Header) Peek(name string) []byte {
	if i := h.index(name); i >= 0 {
		return h.fields[i].value
	}
	return nil
}

// PeekAll returns the values of every field with the given name, in order.
// It allocates the returned slice; the values follow the same rules as [Header.Peek].
func (h *Header) PeekAll(name string) [][]byte {
	var vs [][]byte
	for i := h.index(name); i >= 0 && i < len(h.fields); i++ {
		if bytesconv.EqualFoldLower(h.fields[i].name, name) {
			vs = append(vs, h.fields[i].value)
		}
	}
	return vs
}

// Get returns a copy of the first value for name, or "" if there is none.
// Unlike [Header.Peek] the result stays valid after the handler returns, at the cost of an allocation.
func (h *Header) Get(name string) string {
	return string(h.Peek(name))
}

// Set replaces every field named name with one field holding value. It panics if name is not an
// RFC 9110 token, and replaces CR, LF, and NUL in value with spaces to prevent header injection.
func (h *Header) Set(name, value string) {
	h.SetBytes(name, bytesconv.S2B(value))
}

// SetBytes is like [Header.Set] but takes the value as bytes. value is copied.
func (h *Header) SetBytes(name string, value []byte) {
	mustValidName(name)
	i := h.index(name)
	if i < 0 {
		h.appendField(bytesconv.S2B(name), value)
		return
	}
	h.fields[i].value = sanitizeValue(append(h.fields[i].value[:0], value...))
	h.delFrom(name, i+1)
}

// Add appends a field. Existing fields with the same name are kept.
// It panics and sanitizes like [Header.Set].
func (h *Header) Add(name, value string) {
	h.AddBytes(name, bytesconv.S2B(value))
}

// AddBytes is like [Header.Add] but takes the value as bytes. value is copied.
func (h *Header) AddBytes(name string, value []byte) {
	mustValidName(name)
	h.appendField(bytesconv.S2B(name), value)
}

// Del removes every field with the given name.
func (h *Header) Del(name string) {
	if i := h.index(name); i >= 0 {
		h.delFrom(name, i)
	}
}

// All returns an iterator over every field in order. Names are lowercase.
// The yielded slices follow the same rules as [Header.Peek].
func (h *Header) All() iter.Seq2[[]byte, []byte] {
	h.live()
	return func(yield func(name, value []byte) bool) {
		for _, f := range h.fields {
			if !yield(f.name, f.value) {
				return
			}
		}
	}
}

// Reset removes all fields while keeping allocated memory for reuse.
func (h *Header) Reset() {
	h.live()
	h.fields = h.fields[:0]
}

// CopyTo replaces dst's fields with a deep copy of h's fields.
func (h *Header) CopyTo(dst *Header) {
	h.live()
	dst.Reset()
	for _, f := range h.fields {
		dst.appendField(f.name, f.value)
	}
}

func (h *Header) index(name string) int {
	h.live()
	for i := range h.fields {
		if bytesconv.EqualFoldLower(h.fields[i].name, name) {
			return i
		}
	}
	return -1
}

// appendField reuses the backing arrays of previously removed fields, so steady-state use does not allocate.
func (h *Header) appendField(name, value []byte) {
	n := len(h.fields)
	if n < cap(h.fields) {
		h.fields = h.fields[:n+1]
	} else {
		h.fields = append(h.fields, field{})
	}
	f := &h.fields[n]
	f.name = bytesconv.ToLower(append(f.name[:0], name...))
	f.value = sanitizeValue(append(f.value[:0], value...))
}

// delFrom removes fields named name at index start and later, preserving order.
// Removed fields are swapped past the end so their buffers are reused by appendField.
func (h *Header) delFrom(name string, start int) {
	w := start
	for i := start; i < len(h.fields); i++ {
		if bytesconv.EqualFoldLower(h.fields[i].name, name) {
			continue
		}
		h.fields[w], h.fields[i] = h.fields[i], h.fields[w]
		w++
	}
	h.fields = h.fields[:w]
}

func (h *Header) live() {
	if h.dead {
		panic("qilat: Header used after its request was released")
	}
}

// addWire appends a field received from a peer. Unlike Add it rejects, rather than sanitizes, invalid input.
func (h *Header) addWire(name, value []byte) error {
	if !validName(bytesconv.B2S(name)) {
		return fmt.Errorf("qilat: invalid header field name %q", name)
	}
	if !validValue(value) {
		return fmt.Errorf("qilat: invalid header field value for %q", name)
	}
	h.appendField(name, value)
	return nil
}

func mustValidName(name string) {
	if !validName(name) {
		panic(fmt.Sprintf("qilat: invalid header field name %q", name))
	}
}

// validName reports whether name is an RFC 9110 token.
func validName(name string) bool {
	if name == "" {
		return false
	}
	for i := 0; i < len(name); i++ {
		if !isTokenChar(name[i]) {
			return false
		}
	}
	return true
}

// validValue reports whether v contains none of the bytes that could split or truncate a header line.
func validValue(v []byte) bool {
	for _, c := range v {
		if c == '\r' || c == '\n' || c == 0 {
			return false
		}
	}
	return true
}

func sanitizeValue(v []byte) []byte {
	for i, c := range v {
		if c == '\r' || c == '\n' || c == 0 {
			v[i] = ' '
		}
	}
	return v
}

var tokenTable = func() (t [256]bool) {
	for c := '0'; c <= '9'; c++ {
		t[c] = true
	}
	for c := 'a'; c <= 'z'; c++ {
		t[c] = true
		t[c-'a'+'A'] = true
	}
	for _, c := range "!#$%&'*+-.^_`|~" {
		t[c] = true
	}
	return t
}()

func isTokenChar(c byte) bool { return tokenTable[c] }

// headerList exposes a Header to protocol implementations without an allocation.
type headerList Header

func (l *headerList) Len() int { return len(l.fields) }

func (l *headerList) At(i int) (name, value []byte) {
	f := &l.fields[i]
	return f.name, f.value
}
