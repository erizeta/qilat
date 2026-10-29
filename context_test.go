package qilat

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"testing"
	"time"
)

// newCtxRequest returns an exchange whose request has the given base context.
func newCtxRequest(base context.Context) (*serverExchange, *Request) {
	x := acquireExchange(&exchangeConfig{handler: HandlerFunc(func(ResponseWriter, *Request) {}), baseCtx: base})
	return x, &x.req
}

func isDone(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return true
	case <-time.After(time.Second):
		return false
	}
}

func TestContextCancel(t *testing.T) {
	x, r := newCtxRequest(context.Background())
	ctx := r.Context()
	if ctx.Err() != nil {
		t.Fatalf("Err before cancel = %v", ctx.Err())
	}
	select {
	case <-ctx.Done():
		t.Fatal("Done closed before cancel")
	default:
	}
	if r.Context() != ctx {
		t.Fatal("Context is not stable")
	}
	x.Cancel()
	if !isDone(ctx) || !errors.Is(ctx.Err(), context.Canceled) || !errors.Is(context.Cause(ctx), context.Canceled) {
		t.Fatalf("after cancel: Err %v Cause %v", ctx.Err(), context.Cause(ctx))
	}
	x.Release()
}

func TestContextCanceledBeforeFirstUse(t *testing.T) {
	x, r := newCtxRequest(context.Background())
	x.Cancel()
	ctx := r.Context()
	if !errors.Is(ctx.Err(), context.Canceled) || !isDone(ctx) {
		t.Fatalf("Err = %v", ctx.Err())
	}
	x.Release()
}

func TestContextSurvivesRelease(t *testing.T) {
	x, r := newCtxRequest(context.Background())
	ctx := r.Context()
	x.Release()
	// The pooled request may now serve someone else; the old context must stay canceled.
	x2, r2 := newCtxRequest(context.Background())
	defer x2.Release()
	if !errors.Is(ctx.Err(), context.Canceled) {
		t.Fatalf("old context Err = %v", ctx.Err())
	}
	if r2.Context() == ctx || r2.Context().Err() != nil {
		t.Fatal("new request reused or inherited the old context")
	}
}

func TestContextCanceledWhenHandlerReturns(t *testing.T) {
	var ctx context.Context
	if _, err := serve(t, request{}, func(w ResponseWriter, r *Request) { ctx = r.Context() }); err != nil {
		t.Fatal(err)
	}
	if !errors.Is(ctx.Err(), context.Canceled) {
		t.Fatalf("Err after ServeHTTP = %v", ctx.Err())
	}
}

func TestContextParent(t *testing.T) {
	type key struct{}
	deadline := time.Now().Add(time.Hour)
	parent, cancel := context.WithDeadline(context.WithValue(context.Background(), key{}, "v"), deadline)
	x, r := newCtxRequest(parent)
	defer x.Release()
	ctx := r.Context()
	if ctx.Value(key{}) != "v" {
		t.Fatal("Value not inherited")
	}
	if d, ok := ctx.Deadline(); !ok || !d.Equal(deadline) {
		t.Fatalf("Deadline = %v, %v", d, ok)
	}
	done := ctx.Done()
	cancel()
	if !isDone(ctx) || !errors.Is(ctx.Err(), context.Canceled) {
		t.Fatalf("parent cancel not propagated: %v", ctx.Err())
	}
	if done != ctx.Done() {
		t.Fatal("Done channel changed")
	}
}

func TestContextParentAlreadyCanceled(t *testing.T) {
	parent, cancel := context.WithCancel(context.Background())
	cancel()
	x, r := newCtxRequest(parent)
	defer x.Release()
	if ctx := r.Context(); !isDone(ctx) || !errors.Is(ctx.Err(), context.Canceled) {
		t.Fatalf("Err = %v", ctx.Err())
	}
}

func TestContextErrBeforeDone(t *testing.T) {
	parent, cancel := context.WithTimeout(context.Background(), time.Hour)
	x, r := newCtxRequest(parent)
	defer x.Release()
	ctx := r.Context()
	cancel()
	// Err is checked first: it must close Done itself instead of waiting for the parent watcher.
	if !errors.Is(ctx.Err(), context.Canceled) {
		t.Fatalf("Err = %v", ctx.Err())
	}
	select {
	case <-ctx.Done():
	default:
		t.Fatal("Err reported an error while Done was open")
	}
}

func TestContextDerived(t *testing.T) {
	x, r := newCtxRequest(context.Background())
	child, cancelChild := context.WithTimeout(r.Context(), time.Hour)
	defer cancelChild()
	grandchild, cancelGrandchild := context.WithCancel(child)
	defer cancelGrandchild()
	x.Cancel()
	if !isDone(child) || !isDone(grandchild) {
		t.Fatal("derived contexts not canceled")
	}
	x.Release()
}

func TestContextAfterFunc(t *testing.T) {
	x, r := newCtxRequest(context.Background())
	ctx := r.Context()
	ran := make(chan struct{})
	context.AfterFunc(ctx, func() { close(ran) })
	stopped := make(chan struct{})
	stop := context.AfterFunc(ctx, func() { close(stopped) })
	if !stop() {
		t.Fatal("stop before cancel returned false")
	}
	if stop() {
		t.Fatal("second stop returned true")
	}
	x.Cancel()
	select {
	case <-ran:
	case <-time.After(time.Second):
		t.Fatal("AfterFunc did not run")
	}
	select {
	case <-stopped:
		t.Fatal("stopped AfterFunc ran")
	case <-time.After(50 * time.Millisecond):
	}
	late := make(chan struct{})
	lateStop := context.AfterFunc(ctx, func() { close(late) })
	select {
	case <-late:
	case <-time.After(time.Second):
		t.Fatal("AfterFunc on canceled context did not run")
	}
	if lateStop() {
		t.Fatal("stop after run returned true")
	}
	x.Release()
}

func TestContextString(t *testing.T) {
	x, r := newCtxRequest(context.Background())
	defer x.Release()
	if s := r.Context().(interface{ String() string }).String(); s != "qilat.requestContext" {
		t.Fatalf("String = %q", s)
	}
}

// TestContextConcurrentCancel exercises the Context/Cancel handshake; run with -race.
func TestContextConcurrentCancel(t *testing.T) {
	for range 200 {
		x, r := newCtxRequest(context.Background())
		var ctx context.Context
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); ctx = r.Context() }()
		go func() { defer wg.Done(); x.Cancel() }()
		wg.Wait()
		if !errors.Is(ctx.Err(), context.Canceled) {
			t.Fatal("context left live after concurrent cancel")
		}
		x.Release()
	}
}

// TestContextNoGoroutineLeak covers the parent watcher and AfterFunc paths, which start goroutines.
func TestContextNoGoroutineLeak(t *testing.T) {
	before := runtime.NumGoroutine()
	for range 100 {
		parent, cancel := context.WithCancel(context.Background())
		x, r := newCtxRequest(parent)
		ctx := r.Context()
		_ = ctx.Done()
		child, cancelChild := context.WithCancel(ctx)
		context.AfterFunc(ctx, func() {})
		x.Cancel()
		<-child.Done()
		cancelChild()
		cancel()
		x.Release()
	}
	deadline := time.Now().Add(2 * time.Second)
	for runtime.NumGoroutine() > before && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if n := runtime.NumGoroutine(); n > before {
		t.Fatalf("goroutines: %d before, %d after", before, n)
	}
}
