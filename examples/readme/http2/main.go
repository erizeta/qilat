package main

import (
	"log"

	"github.com/erizeta/qilat"
)

func main() {
	h := qilat.HandlerFunc(func(w qilat.ResponseWriter, r *qilat.Request) {
		_, _ = w.WriteString("hello over " + r.Proto() + "\n")
		// Trailers are sent after the body, as gRPC does with grpc-status.
		w.(qilat.TrailerSetter).Trailer().Set("X-Checksum", "abc123")
	})
	srv := &qilat.Server{Handler: h, H2C: true}
	log.Fatal(srv.ListenAndServe(":8080"))
}
