// Command h3-dev-cert serves HTTP/1.1, HTTP/2, and HTTP/3 on one port with a generated development certificate.
// HTTP/1.1 and HTTP/2 responses carry Alt-Svc, so browsers switch to HTTP/3 on a later request.
package main

import (
	"crypto/tls"
	"log"

	"github.com/erizeta/qilat"
)

func main() {
	cert, err := qilat.GenerateDevCert() // localhost, 127.0.0.1, ::1; for local testing only
	if err != nil {
		log.Fatal(err)
	}
	srv := &qilat.Server{
		Handler: qilat.HandlerFunc(func(w qilat.ResponseWriter, r *qilat.Request) {
			w.Header().Set("Content-Type", "text/plain; charset=utf-8")
			_, _ = w.WriteString("hello over " + r.Proto() + "\n")
		}),
		TLSConfig: &tls.Config{Certificates: []tls.Certificate{cert}},
	}
	log.Println("listening on https://localhost:8443 (TCP: HTTP/1.1 and HTTP/2, UDP: HTTP/3)")
	log.Fatal(srv.ListenAndServeTLS(":8443", "", ""))
}
