package main

import (
	"log"
	"os"

	"github.com/erizeta/qilat"
)

func main() {
	mux := qilat.NewServeMux()
	mux.Handle("GET /static/", qilat.StripPrefix("/static", qilat.FileServer(os.DirFS("public"))))
	log.Fatal((&qilat.Server{Handler: qilat.Compress(0)(mux)}).ListenAndServe(":8080"))
}
