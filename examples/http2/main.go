// Command http2 serves HTTP/2 over TLS (with HTTP/1.1 fallback) and cleartext HTTP/2 (h2c) with qilat.
// It generates a self-signed certificate at startup, so it is for local testing only.
package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"log"
	"math/big"
	"time"

	"github.com/erizeta/qilat"
)

func main() {
	mux := qilat.NewServeMux()
	mux.HandleFunc("GET /{$}", func(w qilat.ResponseWriter, r *qilat.Request) {
		_, _ = w.WriteString("hello over " + r.Proto() + "\n")
	})
	// Trailers are sent after the body, as gRPC does with grpc-status.
	mux.HandleFunc("GET /trailers", func(w qilat.ResponseWriter, _ *qilat.Request) {
		_, _ = w.WriteString("body\n")
		w.(qilat.TrailerSetter).Trailer().Set("X-Checksum", "abc123")
	})

	cert, err := devCertificate()
	if err != nil {
		log.Fatal(err)
	}
	tlsSrv := &qilat.Server{
		Handler:   mux,
		TLSConfig: &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12},
	}
	go func() {
		log.Println("HTTPS (h2 + http/1.1) on https://localhost:8443")
		log.Fatal(tlsSrv.ListenAndServeTLS(":8443", "", ""))
	}()

	h2cSrv := &qilat.Server{Handler: mux, H2C: true}
	log.Println("cleartext (h2c + http/1.1) on http://localhost:8080")
	log.Fatal(h2cSrv.ListenAndServe(":8080"))
}

// devCertificate returns a self-signed certificate for localhost.
func devCertificate() (tls.Certificate, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return tls.Certificate{}, err
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		DNSNames:     []string{"localhost"},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		return tls.Certificate{}, err
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}, nil
}
