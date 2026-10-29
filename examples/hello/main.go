// Command hello serves a minimal HTTP/1.1 application with qilat.
package main

import (
	"context"
	"errors"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/erizeta/qilat"
)

func main() {
	srv := &qilat.Server{
		Handler: qilat.HandlerFunc(func(w qilat.ResponseWriter, r *qilat.Request) {
			name := r.URI().Query().Get("name")
			if name == "" {
				name = "World"
			}
			w.Header().Set("Content-Type", "text/plain; charset=utf-8")
			_, _ = w.WriteString("Hello, " + name + "!\n")
		}),
	}

	go func() {
		log.Println("listening on http://localhost:8080")
		if err := srv.ListenAndServe(":8080"); !errors.Is(err, qilat.ErrServerClosed) {
			log.Fatal(err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		log.Fatal(err)
	}
}
