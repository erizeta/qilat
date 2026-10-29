package qilat

import (
	"context"
	"slices"
	"sync"
	"sync/atomic"
	"time"
)

// requestContext is the context returned by [Request.Context]. It is allocated on first use and never reused,
// so goroutines that keep it past the handler see it canceled rather than attached to a later request.
type requestContext struct {
	parent context.Context

	mu         sync.Mutex
	done       chan struct{} // created lazily by Done
	err        error
	stopParent func() bool
	afters     []*afterFunc
}

type afterFunc struct{ f func() }

func (c *requestContext) Deadline() (time.Time, bool) { return c.parent.Deadline() }

func (c *requestContext) Value(key any) any { return c.parent.Value(key) }

func (c *requestContext) Done() <-chan struct{} {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.done == nil {
		c.done = make(chan struct{})
		if c.err == nil {
			c.err = c.parent.Err()
		}
		if c.err != nil {
			close(c.done)
		} else {
			c.watchParentLocked()
		}
	}
	return c.done
}

func (c *requestContext) Err() error {
	c.mu.Lock()
	err := c.err
	c.mu.Unlock()
	if err != nil {
		return err
	}
	// Cancel synchronously so Done is already closed whenever Err reports an error.
	if err := c.parent.Err(); err != nil {
		c.cancel(err)
		c.mu.Lock()
		err = c.err
		c.mu.Unlock()
		return err
	}
	return nil
}

// AfterFunc lets context.WithCancel and friends propagate cancellation without starting a goroutine.
func (c *requestContext) AfterFunc(f func()) func() bool {
	a := &afterFunc{f: f}
	c.mu.Lock()
	if c.err != nil {
		c.mu.Unlock()
		go f()
		return func() bool { return false }
	}
	c.afters = append(c.afters, a)
	c.watchParentLocked()
	c.mu.Unlock()
	return func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		if i := slices.Index(c.afters, a); i >= 0 {
			c.afters = slices.Delete(c.afters, i, i+1)
			return true
		}
		return false
	}
}

func (c *requestContext) String() string { return "qilat.requestContext" }

func (c *requestContext) watchParentLocked() {
	if c.stopParent == nil && c.parent.Done() != nil {
		c.stopParent = context.AfterFunc(c.parent, func() { c.cancel(c.parent.Err()) })
	}
}

func (c *requestContext) cancel(err error) {
	c.mu.Lock()
	if c.err != nil {
		c.mu.Unlock()
		return
	}
	c.err = err
	if c.done != nil {
		close(c.done)
	}
	afters := c.afters
	c.afters = nil
	stop := c.stopParent
	c.stopParent = nil
	c.mu.Unlock()
	if stop != nil {
		stop()
	}
	for _, a := range afters {
		go a.f()
	}
}

// requestState is shared by a request and its WithContext copies, and reused with the request.
type requestState struct {
	ctx      atomic.Pointer[requestContext]
	canceled atomic.Bool
}

func (s *requestState) context(parent context.Context) context.Context {
	if c := s.ctx.Load(); c != nil {
		return c
	}
	c := &requestContext{parent: parent}
	if !s.ctx.CompareAndSwap(nil, c) {
		return s.ctx.Load()
	}
	// Pairs with cancel: one side always observes the other's store, so the context is never left live.
	if s.canceled.Load() {
		c.cancel(context.Canceled)
	}
	return c
}

func (s *requestState) cancel() {
	s.canceled.Store(true)
	if c := s.ctx.Load(); c != nil {
		c.cancel(context.Canceled)
	}
}

func (s *requestState) reset() {
	s.ctx.Store(nil)
	s.canceled.Store(false)
}
