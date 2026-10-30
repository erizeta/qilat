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
