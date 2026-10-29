// Package bytesconv provides allocation-free conversions and ASCII helpers for HTTP parsing.
package bytesconv

import "unsafe"

// B2S returns a string sharing b's memory. The caller must not modify b while the string is in use.
func B2S(b []byte) string {
	return unsafe.String(unsafe.SliceData(b), len(b)) //nolint:gosec // zero-copy view; callers keep b unmodified
}

// S2B returns a byte slice sharing s's memory. The result must never be modified.
func S2B(s string) []byte {
	return unsafe.Slice(unsafe.StringData(s), len(s)) //nolint:gosec // read-only view of s
}

// ToLower lowercases ASCII letters in b in place and returns b.
func ToLower(b []byte) []byte {
	for i, c := range b {
		if 'A' <= c && c <= 'Z' {
			b[i] = c + ('a' - 'A')
		}
	}
	return b
}

// EqualFoldLower reports whether s equals lower under ASCII case folding.
// lower must already be lowercase.
func EqualFoldLower(lower []byte, s string) bool {
	if len(lower) != len(s) {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if 'A' <= c && c <= 'Z' {
			c += 'a' - 'A'
		}
		if lower[i] != c {
			return false
		}
	}
	return true
}

// ParseUint parses a non-empty run of ASCII digits with no sign or whitespace.
// It reports false on any other input or when the value does not fit in an int64.
func ParseUint(b []byte) (int64, bool) {
	if len(b) == 0 {
		return 0, false
	}
	var n int64
	for _, c := range b {
		if c < '0' || c > '9' {
			return 0, false
		}
		d := int64(c - '0')
		if n > (1<<63-1-d)/10 {
			return 0, false
		}
		n = n*10 + d
	}
	return n, true
}

// Unhex returns the value of the hexadecimal digit c.
func Unhex(c byte) (byte, bool) {
	switch {
	case '0' <= c && c <= '9':
		return c - '0', true
	case 'a' <= c && c <= 'f':
		return c - 'a' + 10, true
	case 'A' <= c && c <= 'F':
		return c - 'A' + 10, true
	}
	return 0, false
}
