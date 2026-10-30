package qilat_test

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/erizeta/qilat"
)

// serve runs h on a loopback port and returns its base URL and a function that stops it.
func serve(h qilat.Handler) (string, func()) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}
	srv := &qilat.Server{Handler: h}
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = srv.Serve(ln)
	}()
	return "http://" + ln.Addr().String(), func() {
		_ = srv.Close()
		<-done
	}
}

// get fetches url with a client of its own and returns the status and body.
func get(url string) (int, string) {
	tr := &qilat.Transport{}
	defer tr.CloseIdleConnections()
	req := qilat.AcquireRequest()
	defer qilat.ReleaseRequest(req)
	if err := req.SetRequestURI(url); err != nil {
		panic(err)
	}
	resp := qilat.AcquireResponse()
	defer qilat.ReleaseResponse(resp)
	if err := (&qilat.Client{Transport: tr}).Do(req, resp); err != nil {
		panic(err)
	}
	body, err := resp.ReadBody()
	if err != nil {
		panic(err)
	}
	return resp.StatusCode(), string(body)
}

func ExampleServeMux() {
	mux := qilat.NewServeMux()
	mux.HandleFunc("GET /users/{id}", func(w qilat.ResponseWriter, r *qilat.Request) {
		_, _ = w.WriteString("user " + r.PathValue("id"))
	})
	url, stop := serve(mux)
	defer stop()

	fmt.Println(get(url + "/users/42"))
	fmt.Println(get(url + "/nope"))
	// Output:
	// 200 user 42
	// 404 404 page not found
}

func ExampleServer_Shutdown() {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}
	srv := &qilat.Server{Handler: qilat.HandlerFunc(func(w qilat.ResponseWriter, _ *qilat.Request) {
		_, _ = w.WriteString("ok")
	})}
	served := make(chan error, 1)
	go func() { served <- srv.Serve(ln) }()
	fmt.Println(get("http://" + ln.Addr().String()))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		panic(err)
	}
	fmt.Println(<-served)
	// Output:
	// 200 ok
	// qilat: Server closed
}

func ExampleClient_Do() {
	url, stop := serve(qilat.HandlerFunc(func(w qilat.ResponseWriter, r *qilat.Request) {
		_, _ = w.WriteString(r.Method() + " " + r.Header.Get("X-Request-Id"))
	}))
	defer stop()

	tr := &qilat.Transport{}
	defer tr.CloseIdleConnections()
	client := &qilat.Client{Transport: tr, Timeout: 5 * time.Second}

	// Requests and responses are pooled; release them when done.
	req := qilat.AcquireRequest()
	defer qilat.ReleaseRequest(req)
	req.SetMethod("DELETE")
	req.Header.Set("X-Request-Id", "abc")
	if err := req.SetRequestURI(url + "/items/1"); err != nil {
		panic(err)
	}
	resp := qilat.AcquireResponse()
	defer qilat.ReleaseResponse(resp)
	if err := client.Do(req, resp); err != nil {
		panic(err)
	}
	body, err := resp.ReadBody() // owned by resp: valid until ReleaseResponse
	if err != nil {
		panic(err)
	}
	fmt.Println(resp.Proto(), resp.StatusCode(), string(body))
	// Output: HTTP/1.1 200 DELETE abc
}

func ExampleFromStd() {
	std := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprintf(w, "net/http handler at %s", r.URL.Path)
	})
	url, stop := serve(qilat.FromStd(std))
	defer stop()

	fmt.Println(get(url + "/legacy"))
	// Output: 200 net/http handler at /legacy
}

func ExampleChain() {
	tag := func(name string) func(qilat.Handler) qilat.Handler {
		return func(next qilat.Handler) qilat.Handler {
			return qilat.HandlerFunc(func(w qilat.ResponseWriter, r *qilat.Request) {
				w.Header().Add("X-Order", name)
				next.ServeHTTP(w, r)
			})
		}
	}
	h := qilat.Chain(tag("first"), tag("second"))(qilat.HandlerFunc(func(w qilat.ResponseWriter, _ *qilat.Request) {
		var order []string
		for _, v := range w.Header().PeekAll("X-Order") {
			order = append(order, string(v))
		}
		_, _ = w.WriteString(strings.Join(order, " then "))
	}))
	url, stop := serve(h)
	defer stop()

	fmt.Println(get(url))
	// Output: 200 first then second
}
