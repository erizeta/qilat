//go:build !go1.27

package qilat

// stdRedirectEscaping is false before Go 1.27, whose ServeMux redirects decode %2F into "/" and escape "%" again.
const stdRedirectEscaping = false
