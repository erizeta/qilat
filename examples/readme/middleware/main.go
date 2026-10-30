package main

import (
	"log"
	"net/http"
	"time"

	"github.com/erizeta/qilat"
)

func logging(next qilat.Handler) qilat.Handler {
	return qilat.HandlerFunc(func(w qilat.ResponseWriter, r *qilat.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		log.Printf("%s %s %v", r.Method(), r.Path(), time.Since(start))
	})
}

func requireToken(next qilat.Handler) qilat.Handler {
	return qilat.HandlerFunc(func(w qilat.ResponseWriter, r *qilat.Request) {
		if string(r.Header.Peek("Authorization")) != "Bearer secret" {
			qilat.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func main() {
	mux := qilat.NewServeMux()
	mux.HandleFunc("GET /", func(w qilat.ResponseWriter, _ *qilat.Request) {
		_, _ = w.WriteString("welcome\n")
	})
	h := qilat.Chain(logging, requireToken, qilat.Compress(0))(mux)
	log.Fatal((&qilat.Server{Handler: h}).ListenAndServe(":8080"))
}
