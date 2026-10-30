// Command h2loadserver serves the same plaintext handler with net/http (port 19001) and qilat (port 19002), both
// speaking HTTP/1.1 and cleartext HTTP/2, for out-of-process load tests such as h2load. See RESULTS.md.
package main

import (
	"log"
	"net/http"
	"time"

	"github.com/erizeta/qilat"
)

var body = []byte("Hello, World!")

func main() {
	go func() {
		var p http.Protocols
		p.SetHTTP1(true)
		p.SetUnencryptedHTTP2(true)
		srv := &http.Server{
			Addr:              "127.0.0.1:19001",
			Protocols:         &p,
			ReadHeaderTimeout: 10 * time.Second,
			Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "text/plain")
				_, _ = w.Write(body)
			}),
		}
		log.Fatal(srv.ListenAndServe())
	}()
	srv := &qilat.Server{H2C: true, Handler: qilat.HandlerFunc(func(w qilat.ResponseWriter, _ *qilat.Request) {
		w.Header().Set("Content-Type", "text/plain")
		_, _ = w.Write(body)
	})}
	log.Fatal(srv.ListenAndServe("127.0.0.1:19002"))
}
