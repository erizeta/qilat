// Command routing serves a small in-memory key-value API with qilat's ServeMux.
package main

import (
	"io"
	"log"
	"sync"

	"github.com/erizeta/qilat"
)

func main() {
	var (
		mu    sync.Mutex
		items = map[string]string{}
	)

	mux := qilat.NewServeMux()
	mux.HandleFunc("GET /{$}", func(w qilat.ResponseWriter, _ *qilat.Request) {
		_, _ = w.WriteString("try: curl -X PUT -d hello localhost:8080/items/greeting\n")
	})
	mux.HandleFunc("GET /items/{key}", func(w qilat.ResponseWriter, r *qilat.Request) {
		mu.Lock()
		v, ok := items[string(r.PeekPathValue("key"))] // the map lookup does not keep the bytes
		mu.Unlock()
		if !ok {
			w.WriteHeader(404)
			_, _ = w.WriteString("not found\n")
			return
		}
		_, _ = w.WriteString(v + "\n")
	})
	mux.HandleFunc("PUT /items/{key}", func(w qilat.ResponseWriter, r *qilat.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(400)
			return
		}
		mu.Lock()
		items[r.PathValue("key")] = string(body) // PathValue returns a copy that is safe to keep
		mu.Unlock()
		w.WriteHeader(204)
	})
	mux.HandleFunc("DELETE /items/{key}", func(w qilat.ResponseWriter, r *qilat.Request) {
		mu.Lock()
		delete(items, string(r.PeekPathValue("key")))
		mu.Unlock()
		w.WriteHeader(204)
	})
	mux.HandleFunc("GET /files/{path...}", func(w qilat.ResponseWriter, r *qilat.Request) {
		_, _ = w.WriteString("pattern " + r.Pattern() + ", path " + r.PathValue("path") + "\n")
	})

	srv := &qilat.Server{Handler: mux}
	log.Println("listening on http://localhost:8080")
	log.Fatal(srv.ListenAndServe(":8080"))
}
