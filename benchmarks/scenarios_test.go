package benchmarks

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
)

const (
	plaintextBody = "Hello, World!"
	uploadSize    = 64 << 10
	headerCount   = 20
	targetHeader  = "X-Bench-Target"
	targetValue   = "qilat-benchmark-value"
)

var (
	jsonBody   = buildJSONBody()
	uploadBody = bytes.Repeat([]byte("q"), uploadSize)
)

// scenario describes one request shape; every server under test must answer it identically.
type scenario struct {
	name     string
	method   string
	path     string
	headers  [][2]string
	body     []byte
	wantBody []byte
}

var scenarios = []scenario{
	{name: "plaintext", method: "GET", path: "/plaintext", wantBody: []byte(plaintextBody)},
	{name: "json-1kb", method: "GET", path: "/json", wantBody: jsonBody},
	{name: "headers-20", method: "GET", path: "/headers", headers: benchHeaders(), wantBody: []byte(targetValue)},
	{name: "upload-64kb", method: "POST", path: "/upload", body: uploadBody, wantBody: []byte(strconv.Itoa(uploadSize))},
}

// rawRequest renders the scenario as HTTP/1.1 bytes so the H1 client does no per-request formatting.
func (s scenario) rawRequest() []byte {
	var b bytes.Buffer
	fmt.Fprintf(&b, "%s %s HTTP/1.1\r\nHost: bench\r\n", s.method, s.path)
	for _, h := range s.headers {
		fmt.Fprintf(&b, "%s: %s\r\n", h[0], h[1])
	}
	if s.body != nil {
		fmt.Fprintf(&b, "Content-Type: application/octet-stream\r\nContent-Length: %d\r\n", len(s.body))
	}
	b.WriteString("\r\n")
	b.Write(s.body)
	return b.Bytes()
}

func benchHeaders() [][2]string {
	hs := make([][2]string, 0, headerCount)
	for i := range headerCount - 1 {
		hs = append(hs, [2]string{fmt.Sprintf("X-Bench-Header-%02d", i), strings.Repeat("v", 24)})
	}
	return append(hs, [2]string{targetHeader, targetValue})
}

func buildJSONBody() []byte {
	var b bytes.Buffer
	b.WriteString(`{"items":[`)
	for i := 0; b.Len() < 1000; i++ {
		if i > 0 {
			b.WriteByte(',')
		}
		fmt.Fprintf(&b, `{"id":%d,"name":"item-%03d","ok":true}`, i, i)
	}
	b.WriteString(`]}`)
	return b.Bytes()
}
