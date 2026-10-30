package h2

import (
	"sync"
	"sync/atomic"
)

// writeSched serializes frame writes. It is a lock whose waiters are handed the connection in RFC 9218
// priority order: lowest urgency first; within an urgency, non-incremental streams in stream ID order,
// then the rest first come, first served. Writers hold it for at most one frame's worth of body, so
// streams interleave at frame granularity. An uncontended lock costs no channel operation.
type writeSched struct {
	mu      sync.Mutex
	busy    bool
	waiters []*waiter
}

// waiter is owned by one goroutine (a stream's handler or the connection reader) and reused for every write.
type waiter struct {
	ch       chan struct{}
	id       uint32
	priority *atomic.Uint32 // urgency<<1 | incremental; nil for control frames, which always go first
}

func newWaiter(id uint32, priority *atomic.Uint32) *waiter {
	return &waiter{ch: make(chan struct{}, 1), id: id, priority: priority}
}

func (w *waiter) prio() (urgency uint32, incremental bool) {
	if w.priority == nil {
		return 0, false
	}
	p := w.priority.Load()
	return p >> 1, p&1 != 0
}

func (s *writeSched) lock(w *waiter) {
	s.mu.Lock()
	if !s.busy {
		s.busy = true
		s.mu.Unlock()
		return
	}
	s.waiters = append(s.waiters, w)
	s.mu.Unlock()
	<-w.ch
}

// hasWaiters reports whether another writer is queued, in which case it will flush for us.
func (s *writeSched) hasWaiters() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.waiters) > 0
}

func (s *writeSched) unlock() {
	s.mu.Lock()
	if len(s.waiters) == 0 {
		s.busy = false
		s.mu.Unlock()
		return
	}
	best := 0
	bu, binc := s.waiters[0].prio()
	for i, w := range s.waiters[1:] {
		u, inc := w.prio()
		if u < bu || u == bu && !inc && (binc || w.id < s.waiters[best].id) {
			best, bu, binc = i+1, u, inc
		}
	}
	w := s.waiters[best]
	copy(s.waiters[best:], s.waiters[best+1:])
	s.waiters[len(s.waiters)-1] = nil
	s.waiters = s.waiters[:len(s.waiters)-1]
	s.mu.Unlock()
	w.ch <- struct{}{}
}

// parsePriority reads urgency and incremental from an RFC 9218 Priority field value such as "u=1, i".
// Unknown or malformed members are ignored, as the RFC requires.
func parsePriority(v []byte, urgency uint32, incremental bool) (uint32, bool) {
	for len(v) > 0 {
		var member []byte
		member, v = cutByte(v, ',')
		item, _ := cutByte(trimSpace(member), ';') // parameters are ignored
		key, val := cutByte(item, '=')
		switch string(key) {
		case "u":
			if len(val) == 1 && '0' <= val[0] && val[0] <= '7' {
				urgency = uint32(val[0] - '0')
			}
		case "i":
			switch {
			case len(val) == 0 && len(item) == 1, string(val) == "?1":
				incremental = true
			case string(val) == "?0":
				incremental = false
			}
		}
	}
	return urgency, incremental
}

func cutByte(b []byte, sep byte) (before, after []byte) {
	for i, c := range b {
		if c == sep {
			return b[:i], b[i+1:]
		}
	}
	return b, nil
}

func trimSpace(b []byte) []byte {
	for len(b) > 0 && (b[0] == ' ' || b[0] == '\t') {
		b = b[1:]
	}
	for len(b) > 0 && (b[len(b)-1] == ' ' || b[len(b)-1] == '\t') {
		b = b[:len(b)-1]
	}
	return b
}
