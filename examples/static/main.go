// Command static serves the files embedded from ./public with gzip compression, next to a small JSON API.
package main

import (
	"embed"
	"io/fs"
	"log"

	"github.com/erizeta/qilat"
)

//go:embed public
var public embed.FS

func main() {
	files, err := fs.Sub(public, "public")
	if err != nil {
		log.Fatal(err)
	}
	mux := qilat.NewServeMux()
	// /static/app.js is served from public/app.js; Range and If-Modified-Since work as in net/http.
	mux.Handle("GET /static/", qilat.StripPrefix("/static", qilat.FileServer(files)))
	mux.HandleFunc("GET /api/status", func(w qilat.ResponseWriter, _ *qilat.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.WriteString(`{"status":"ok"}` + "\n")
	})

	srv := &qilat.Server{Handler: qilat.Compress(0)(mux)}
	log.Println("listening on http://localhost:8080/static/")
	log.Fatal(srv.ListenAndServe(":8080"))
}
