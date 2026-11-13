module github.com/erizeta/qilat/benchmarks

go 1.26.0

require (
	github.com/quic-go/quic-go v0.65.0
	github.com/valyala/fasthttp v1.78.0
)

require (
	github.com/quic-go/qpack v0.6.0 // indirect
	golang.org/x/crypto v0.58.0 // indirect
	golang.org/x/net v0.62.0 // indirect
	golang.org/x/sys v0.49.0 // indirect
	golang.org/x/text v0.43.0 // indirect
)

require (
	github.com/erizeta/qilat v0.0.0-00010101000000-000000000000
	github.com/klauspost/compress v1.20.4 // indirect
	github.com/molecule-man/go-brrr v1.3.1 // indirect
	github.com/valyala/bytebufferpool v1.0.0 // indirect
)

replace github.com/erizeta/qilat => ../
