package main

import (
	"context"
	"errors"
	"log"
	"os/signal"
	"syscall"
	"time"

	"github.com/erizeta/qilat"
)

func main() {
	srv := &qilat.Server{Handler: qilat.HandlerFunc(func(w qilat.ResponseWriter, _ *qilat.Request) {
		_, _ = w.WriteString("ok\n")
	})}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	drained := make(chan struct{})
	go func() {
		defer close(drained)
		<-ctx.Done()
		timeout, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := srv.Shutdown(timeout); err != nil {
			log.Print("shutdown: ", err)
		}
	}()

	if err := srv.ListenAndServe(":8080"); !errors.Is(err, qilat.ErrServerClosed) {
		log.Fatal(err)
	}
	<-drained // ListenAndServe returns as soon as Shutdown starts; wait for running requests
}
