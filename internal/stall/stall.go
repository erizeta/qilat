// Package stall detects a client that stops making progress while a handler waits for it.
package stall

import (
	"math"
	"sync/atomic"
	"time"
)

var epoch = time.Now()

// now is monotonic nanoseconds since epoch, offset by one so that zero means "not waiting".
func now() int64 { return int64(time.Since(epoch)) + 1 }

// Watchdog calls its stall function once a single wait has lasted the whole timeout. Begin and End bracket
// each wait and cost a few atomic operations; the timer is created on first use, re-arms itself while waits
// happen, and goes idle otherwise, so a Watchdog that is reused does not allocate.
//
// Begin and End must not be called concurrently with each other.
type Watchdog struct {
	d       atomic.Int64 // the timeout; a check still pending from an earlier use may read it
	onStall func()
	t       *time.Timer
	armed   atomic.Bool
	since   atomic.Int64 // when the current wait began; zero when not waiting
}

// Init sets the function called on a stall. It is called once, before any other method.
func (w *Watchdog) Init(onStall func()) { w.onStall = onStall }

// SetTimeout sets the timeout for the waits that follow; zero or less disables the Watchdog.
func (w *Watchdog) SetTimeout(d time.Duration) { w.d.Store(int64(d)) }

// Begin marks the start of a wait for the client.
func (w *Watchdog) Begin() {
	d := time.Duration(w.d.Load())
	if d <= 0 {
		return
	}
	w.since.Store(now())
	if !w.armed.Swap(true) {
		if w.t == nil {
			// Started only once assigned, so check never sees a nil timer.
			w.t = time.AfterFunc(math.MaxInt64, w.check)
		}
		w.t.Reset(d)
	}
}

// End marks the end of a wait.
func (w *Watchdog) End() {
	if w.d.Load() > 0 {
		w.since.Store(0)
	}
}

func (w *Watchdog) check() {
	d := time.Duration(w.d.Load())
	if s := w.since.Load(); s != 0 {
		waited := time.Duration(now() - s)
		if waited < d {
			w.t.Reset(d - waited) // a later wait than the one this timer was set for
			return
		}
		// Clear the wait only if it is still the same one, then report it.
		if w.since.CompareAndSwap(s, 0) {
			w.armed.Store(false)
			w.onStall()
			return
		}
	}
	w.armed.Store(false)
	// A wait that began after the load above found the timer still armed and did not re-arm it.
	if w.since.Load() != 0 && !w.armed.Swap(true) {
		w.t.Reset(d)
	}
}
