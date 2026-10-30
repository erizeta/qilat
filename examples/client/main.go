// Command client fetches a URL with qilat's Client and prints the protocol, status, and body size.
// HTTP/3 is used from the second request on when the server advertises it with Alt-Svc.
package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/erizeta/qilat"
)

func main() {
	url := "https://www.cloudflare.com/"
	if len(os.Args) > 1 {
		url = os.Args[1]
	}
	client := &qilat.Client{
		Transport: &qilat.Transport{EnableHTTP3: true},
		Timeout:   10 * time.Second,
	}
	req := qilat.AcquireRequest()
	defer qilat.ReleaseRequest(req)
	if err := req.SetRequestURI(url); err != nil {
		log.Fatal(err)
	}
	req.Header.Set("User-Agent", "qilat-example")

	resp := qilat.AcquireResponse()
	defer qilat.ReleaseResponse(resp)
	for i := range 2 {
		if err := client.Do(req, resp); err != nil {
			log.Fatal(err)
		}
		body, err := resp.ReadBody()
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("request %d: %s %d, %d bytes\n", i+1, resp.Proto(), resp.StatusCode(), len(body))
	}
}
