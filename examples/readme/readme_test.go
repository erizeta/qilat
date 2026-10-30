// Package readme holds every Go snippet of the repository README as a program, so CI compiles them.
package readme

import (
	"os"
	"path/filepath"
	"regexp"
	"testing"
)

var goBlock = regexp.MustCompile("(?s)```go\n(.*?)```")

// TestSnippetsMatchReadme fails when a README snippet and its program here differ, so neither goes stale.
func TestSnippetsMatchReadme(t *testing.T) {
	readme, err := os.ReadFile("../../README.md")
	if err != nil {
		t.Fatal(err)
	}
	programs, err := filepath.Glob("*/main.go")
	if err != nil {
		t.Fatal(err)
	}
	unmatched := make(map[string]string, len(programs))
	for _, p := range programs {
		b, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		unmatched[string(b)] = p
	}
	for _, m := range goBlock.FindAllSubmatch(readme, -1) {
		if _, ok := unmatched[string(m[1])]; !ok {
			t.Errorf("README snippet has no identical program in examples/readme:\n%s", m[1])
			continue
		}
		delete(unmatched, string(m[1]))
	}
	for _, p := range unmatched {
		t.Errorf("%s is not in the README", p)
	}
}
