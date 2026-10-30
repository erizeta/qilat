package main

import (
	"fmt"
	"log"
	"time"

	"github.com/erizeta/qilat"
)

func main() {
	client := &qilat.Client{
		Transport: &qilat.Transport{EnableHTTP3: true},
		Timeout:   10 * time.Second,
	}
	req := qilat.AcquireRequest()
	defer qilat.ReleaseRequest(req)
	if err := req.SetRequestURI("https://www.cloudflare.com/"); err != nil {
		log.Fatal(err)
	}
	resp := qilat.AcquireResponse()
	defer qilat.ReleaseResponse(resp)
	for range 2 {
		if err := client.Do(req, resp); err != nil {
			log.Fatal(err)
		}
		body, err := resp.ReadBody()
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(resp.Proto(), resp.StatusCode(), len(body), "bytes")
	}
	// HTTP/2.0 200 ... bytes
	// HTTP/3.0 200 ... bytes  (the first response advertised HTTP/3 with Alt-Svc)
}
