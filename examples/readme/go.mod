module github.com/erizeta/qilat/examples/readme

go 1.26.0

replace github.com/erizeta/qilat => ../..

require (
	github.com/erizeta/qilat v0.0.0-00010101000000-000000000000
	github.com/go-chi/chi/v5 v5.5.0
	github.com/klauspost/compress v1.20.3
	github.com/rs/cors v1.11.1
)

require (
	github.com/quic-go/quic-go v0.64.1 // indirect
	golang.org/x/crypto v0.58.0 // indirect
	golang.org/x/net v0.62.0 // indirect
	golang.org/x/sys v0.49.0 // indirect
	golang.org/x/text v0.43.0 // indirect
)
