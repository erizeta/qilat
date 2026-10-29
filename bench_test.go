package qilat

import "testing"

func BenchmarkHeaderPeek(b *testing.B) {
	var h Header
	for _, name := range []string{"Host", "User-Agent", "Accept", "Accept-Encoding", "Accept-Language", "Cookie", "Content-Type"} {
		h.Set(name, "value")
	}
	b.ReportAllocs()
	for b.Loop() {
		_ = h.Peek("content-type")
	}
}

func BenchmarkHeaderSetAdd(b *testing.B) {
	var h Header
	b.ReportAllocs()
	for b.Loop() {
		h.Reset()
		h.Set("Content-Type", "application/json")
		h.Add("Vary", "Accept-Encoding")
		h.Add("Vary", "Origin")
		h.Set("Cache-Control", "no-store")
	}
}

func BenchmarkURIParse(b *testing.B) {
	var u URI
	target := []byte("/api/v1/users/42?name=qilat&page=2&tag=a+b")
	b.ReportAllocs()
	for b.Loop() {
		_ = u.Parse(target)
		_ = u.Path()
		_ = u.Query().Peek("page")
	}
}

func BenchmarkServeExchange(b *testing.B) {
	body := []byte("Hello, World!")
	cfg := &exchangeConfig{handler: HandlerFunc(func(w ResponseWriter, r *Request) {
		w.Header().Set("Content-Type", "text/plain")
		_, _ = w.Write(body)
	})}
	method, target, host := []byte("GET"), []byte("/plaintext"), []byte("bench")
	b.ReportAllocs()
	for b.Loop() {
		x := acquireExchange(cfg)
		_ = x.SetMethod(method)
		_ = x.SetTarget(target)
		_ = x.SetHost(host)
		_ = x.Serve(discardSink{})
		x.Release()
	}
}
