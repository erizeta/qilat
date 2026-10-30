package main

import (
	"crypto/tls"
	"log"

	"github.com/erizeta/qilat"
)

func main() {
	cert, err := qilat.GenerateDevCert() // self-signed, for local development only
	if err != nil {
		log.Fatal(err)
	}
	srv := &qilat.Server{
		Handler: qilat.HandlerFunc(func(w qilat.ResponseWriter, r *qilat.Request) {
			_, _ = w.WriteString("hello over " + r.Proto() + "\n")
		}),
		TLSConfig: &tls.Config{Certificates: []tls.Certificate{cert}},
	}
	// TCP: HTTP/1.1 and HTTP/2. UDP: HTTP/3. Both on port 8443.
	log.Fatal(srv.ListenAndServeTLS(":8443", "", ""))
}
