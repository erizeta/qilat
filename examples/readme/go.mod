module github.com/erizeta/qilat/examples/readme

go 1.26.0

replace github.com/erizeta/qilat => ../..

require (
	github.com/erizeta/qilat v0.0.0-00010101000000-000000000000
	github.com/go-chi/chi/v5 v5.5.0
	github.com/klauspost/compress v1.20.5
	github.com/rs/cors v1.11.1
)

require (
	github.com/quic-go/quic-go v0.66.0 // indirect
	golang.org/x/crypto v0.59.0 // indirect
	golang.org/x/net v0.63.0 // indirect
	golang.org/x/sys v0.50.0 // indirect
	golang.org/x/text v0.44.0 // indirect
)
