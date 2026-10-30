package main

import (
	"io"
	"log"

	"github.com/erizeta/qilat"
	"github.com/klauspost/compress/zstd"
)

func main() {
	mux := qilat.NewServeMux()
	mux.HandleFunc("GET /", func(w qilat.ResponseWriter, _ *qilat.Request) {
		w.Header().Set("Content-Type", "text/plain")
		for range 100 {
			_, _ = w.WriteString("compressible text\n")
		}
	})
	zstdEncoder := qilat.Encoder{Name: "zstd", New: func(w io.Writer) qilat.CompressWriter {
		enc, _ := zstd.NewWriter(w, zstd.WithEncoderLevel(zstd.SpeedFastest))
		return enc
	}}
	compress := qilat.CompressWith(qilat.CompressConfig{Encoders: []qilat.Encoder{zstdEncoder}})
	log.Fatal((&qilat.Server{Handler: compress(mux)}).ListenAndServe(":8080"))
}
