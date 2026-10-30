package main

import (
	"encoding/json"
	"log"
	"net/http"

	"github.com/erizeta/qilat"
)

func main() {
	mux := qilat.NewServeMux()
	mux.HandleFunc("POST /greet", func(w qilat.ResponseWriter, r *qilat.Request) {
		var in struct {
			Name string `json:"name"`
		}
		if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
			qilat.Error(w, "invalid JSON", http.StatusBadRequest)
			return
		}
		lang := r.URI().Query().Get("lang")  // a string copy
		agent := r.Header.Peek("User-Agent") // bytes valid until the handler returns
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{
			"message": "hello " + in.Name,
			"lang":    lang,
			"agent":   string(agent),
		})
	})
	log.Fatal((&qilat.Server{Handler: mux}).ListenAndServe(":8080"))
}
