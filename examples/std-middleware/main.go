// Command std-middleware runs popular net/http middleware (go-chi/chi and rs/cors) in front of qilat handlers
// on HTTP/1.1, HTTP/2, and HTTP/3. It lives in its own module so qilat itself does not depend on them.
package main

import (
	"crypto/tls"
	"log"
	"os"

	"github.com/erizeta/qilat"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/rs/cors"
)

func newHandler(logger middleware.LoggerInterface) qilat.Handler {
	mux := qilat.NewServeMux()
	mux.HandleFunc("GET /hello/{name}", func(w qilat.ResponseWriter, r *qilat.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = w.WriteString("hello " + r.PathValue("name") + " over " + r.Proto() +
			", request " + middleware.GetReqID(r.Context()) + "\n")
	})
	mux.HandleFunc("GET /panic", func(qilat.ResponseWriter, *qilat.Request) { panic("boom") })

	c := cors.New(cors.Options{
		AllowedOrigins: []string{"https://example.com"},
		AllowedMethods: []string{"GET", "POST"},
	})
	// Compose the net/http middleware first and adapt the chain once: each FromStdMiddleware converts the
	// request to net/http and back.
	std := chi.Chain(
		middleware.RequestID,
		middleware.Heartbeat("/ping"),
		middleware.SetHeader("X-Content-Type-Options", "nosniff"),
		middleware.RequestLogger(&middleware.DefaultLogFormatter{Logger: logger, NoColor: true}),
		middleware.Recoverer,
		c.Handler,
	)
	return qilat.FromStdMiddleware(std.Handler)(mux)
}

func main() {
	cert, err := qilat.GenerateDevCert() // self-signed, for local development only
	if err != nil {
		log.Fatal(err)
	}
	srv := &qilat.Server{
		Handler:   newHandler(log.New(os.Stderr, "", log.LstdFlags)),
		TLSConfig: &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12},
	}
	log.Println("listening on https://localhost:8443 (HTTP/1.1, HTTP/2, and HTTP/3)")
	log.Fatal(srv.ListenAndServeTLS(":8443", "", ""))
}
