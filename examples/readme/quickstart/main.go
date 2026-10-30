package main

import (
	"log"

	"github.com/erizeta/qilat"
)

func main() {
	h := qilat.HandlerFunc(func(w qilat.ResponseWriter, r *qilat.Request) {
		_, _ = w.WriteString("Hello over " + r.Proto() + "!\n")
	})
	log.Fatal((&qilat.Server{Handler: h}).ListenAndServe(":8080"))
}
