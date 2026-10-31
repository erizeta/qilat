package stall

import (
	"sync/atomic"
	"testing"
	"time"
)

const d = 50 * time.Millisecond

func TestStallAfterTimeout(t *testing.T) {
	var w Watchdog
	fired := make(chan time.Duration, 1)
	start := time.Now()
	w.Init(func() { fired <- time.Since(start) })
	w.SetTimeout(d)
	w.Begin()
	select {
	case after := <-fired:
		if after < d {
			t.Fatalf("stall reported after %v, before the %v timeout", after, d)
		}
	case <-time.After(20 * d):
		t.Fatal("no stall reported")
	}
}

// TestShortWaitsDoNotStall waits many times, each shorter than the timeout, over several timeouts in total.
func TestShortWaitsDoNotStall(t *testing.T) {
	var w Watchdog
	var stalls atomic.Int32
	w.Init(func() { stalls.Add(1) })
	w.SetTimeout(d)
	for range 20 {
		w.Begin()
		time.Sleep(d / 4)
		w.End()
		time.Sleep(d / 4) // not waiting for the client: never a stall, however long
	}
	time.Sleep(2 * d)
	if n := stalls.Load(); n != 0 {
		t.Fatalf("%d stalls for waits shorter than the timeout", n)
	}
	// The Watchdog still works after going idle.
	w.Begin()
	time.Sleep(3 * d)
	if n := stalls.Load(); n != 1 {
		t.Fatalf("%d stalls for one long wait, want 1", n)
	}
}

func TestDisabled(t *testing.T) {
	var w Watchdog
	w.Init(func() { t.Error("disabled Watchdog reported a stall") })
	w.Begin()
	time.Sleep(2 * d)
	w.End()
}

func TestNoAllocsOnceArmed(t *testing.T) {
	var w Watchdog
	w.Init(func() {})
	w.SetTimeout(time.Hour)
	w.Begin()
	w.End()
	if n := testing.AllocsPerRun(1000, func() { w.Begin(); w.End() }); n != 0 {
		t.Fatalf("%v allocations per wait", n)
	}
}
