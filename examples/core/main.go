// Command core shows the HTTP building blocks available in qilat today.
package main

import (
	"fmt"

	"github.com/erizeta/qilat"
)

func main() {
	var u qilat.URI
	if err := u.Parse([]byte("/users/j%C3%B6rg?tab=posts&page=2")); err != nil {
		panic(err)
	}
	fmt.Println(string(u.Path()))      // /users/jörg
	fmt.Println(u.Query().Get("page")) // 2

	var h qilat.Header
	h.Set("Content-Type", "application/json")
	h.Add("Vary", "Accept-Encoding")
	h.Add("Vary", "Origin")
	fmt.Println(h.Get("content-type"))  // application/json
	fmt.Println(len(h.PeekAll("vary"))) // 2
}
