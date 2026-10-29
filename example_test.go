package qilat_test

import (
	"fmt"

	"github.com/erizeta/qilat"
)

func ExampleHeader() {
	var h qilat.Header
	h.Set("Content-Type", "application/json")
	h.Add("Vary", "Accept-Encoding")
	h.Add("vary", "Origin")

	fmt.Println(h.Get("CONTENT-TYPE"))
	fmt.Println(len(h.PeekAll("Vary")))
	for name, value := range h.All() {
		fmt.Printf("%s: %s\n", name, value)
	}
	// Output:
	// application/json
	// 2
	// content-type: application/json
	// vary: Accept-Encoding
	// vary: Origin
}

func ExampleURI() {
	var u qilat.URI
	if err := u.Parse([]byte("/users/j%C3%B6rg?tab=posts&page=2")); err != nil {
		panic(err)
	}
	fmt.Println(string(u.RawPath()))
	fmt.Println(string(u.Path()))
	fmt.Println(u.Query().Get("page"))
	// Output:
	// /users/j%C3%B6rg
	// /users/jörg
	// 2
}

func ExampleArgs() {
	var a qilat.Args
	a.Parse([]byte("tag=go&tag=http%2F3&q=fast+server"))
	fmt.Println(a.Get("q"))
	for _, v := range a.PeekAll("tag") {
		fmt.Println(string(v))
	}
	// Output:
	// fast server
	// go
	// http/3
}
