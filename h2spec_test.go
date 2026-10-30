package qilat

import (
	"bytes"
	"io"
	"net"
	"os"
	"os/exec"
	"testing"
)

// TestH2Spec runs the h2spec conformance suite (strict mode) against HTTP/2 over TLS. It runs only when the
// H2SPEC environment variable names the h2spec binary, as in CI:
//
//	go install github.com/summerwind/h2spec/cmd/h2spec@v1.5.1-0.20220625142712-af83a65f0b62
//	H2SPEC=$(go env GOPATH)/bin/h2spec go test -run TestH2Spec .
func TestH2Spec(t *testing.T) {
	bin := os.Getenv("H2SPEC")
	if bin == "" {
		t.Skip("set H2SPEC to the h2spec binary to run the HTTP/2 conformance suite")
	}
	// Some h2spec cases need a response larger than the windows they set.
	body := make([]byte, 2048)
	addr := startTLSServer(t, func(w ResponseWriter, r *Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		_, _ = w.Write(body)
	}, selfSignedCert(t))
	host, port, _ := net.SplitHostPort(addr)
	var out bytes.Buffer
	cmd := exec.Command(bin, "-h", host, "-p", port, "-t", "-k", "-S", "-o", "5") //nolint:gosec // test-only binary from the environment
	cmd.Stdout, cmd.Stderr = &out, &out
	if err := cmd.Run(); err != nil {
		t.Fatalf("h2spec failed: %v\n%s", err, out.Bytes())
	}
	t.Logf("%s", lastLine(out.Bytes()))
}

func lastLine(b []byte) []byte {
	b = bytes.TrimSpace(b)
	if i := bytes.LastIndexByte(b, '\n'); i >= 0 {
		return b[i+1:]
	}
	return b
}
