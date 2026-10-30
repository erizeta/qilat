package main

import (
	"fmt"
	"log"
	"time"

	"github.com/erizeta/qilat"
)

func main() {
	mux := qilat.NewServeMux()
	mux.HandleFunc("GET /events", func(w qilat.ResponseWriter, r *qilat.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		tick := time.NewTicker(time.Second)
		defer tick.Stop()
		for i := 1; ; i++ {
			select {
			case <-r.Context().Done(): // the client went away
				return
			case <-tick.C:
				_, _ = fmt.Fprintf(w, "data: tick %d\n\n", i)
				if err := w.Flush(); err != nil {
					return
				}
			}
		}
	})
	log.Fatal((&qilat.Server{Handler: mux}).ListenAndServe(":8080"))
}
