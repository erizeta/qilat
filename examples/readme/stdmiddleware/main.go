package main

import (
	"log"

	"github.com/erizeta/qilat"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/rs/cors"
)

func main() {
	mux := qilat.NewServeMux()
	mux.HandleFunc("GET /hello/{name}", func(w qilat.ResponseWriter, r *qilat.Request) {
		_, _ = w.WriteString("hello " + r.PathValue("name") + ", request " + middleware.GetReqID(r.Context()) + "\n")
	})
	// Compose the net/http middleware first, then adapt the chain once.
	std := chi.Chain(
		middleware.RequestID,
		middleware.Heartbeat("/ping"),
		middleware.Recoverer,
		cors.New(cors.Options{AllowedOrigins: []string{"https://example.com"}}).Handler,
	)
	h := qilat.FromStdMiddleware(std.Handler)(mux)
	log.Fatal((&qilat.Server{Handler: h}).ListenAndServe(":8080"))
}
