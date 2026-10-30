package httpfields

import "testing"

func TestParsePriority(t *testing.T) {
	for _, tt := range []struct {
		in   string
		u    uint32
		incr bool
	}{
		{"", 3, false}, {"u=0", 0, false}, {"u=7, i", 7, true}, {"i, u=2", 2, true}, {"u=9", 3, false},
		{"i=?0", 3, false}, {"i=?1;x=y", 3, true}, {"u=1;foo, bar", 1, false}, {"u=12", 3, false},
	} {
		if u, i := ParsePriority([]byte(tt.in), 3, false); u != tt.u || i != tt.incr {
			t.Errorf("ParsePriority(%q) = %d, %v; want %d, %v", tt.in, u, i, tt.u, tt.incr)
		}
	}
}
