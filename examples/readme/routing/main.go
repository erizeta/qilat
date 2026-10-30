package main

import (
	"log"

	"github.com/erizeta/qilat"
)

func main() {
	mux := qilat.NewServeMux()
	mux.HandleFunc("GET /users/{id}", func(w qilat.ResponseWriter, r *qilat.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.WriteString(`{"id":"` + r.PathValue("id") + `"}` + "\n")
	})
	mux.HandleFunc("GET /files/{path...}", func(w qilat.ResponseWriter, r *qilat.Request) {
		_, _ = w.WriteString("file: " + r.PathValue("path") + "\n")
	})
	log.Fatal((&qilat.Server{Handler: mux}).ListenAndServe(":8080"))
}
