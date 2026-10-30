//go:build go1.27

package qilat

// stdRedirectEscaping reports whether net/http keeps a request's escaping in ServeMux redirects (Go 1.27+).
const stdRedirectEscaping = true
