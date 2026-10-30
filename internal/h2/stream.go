package h2

import (
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/erizeta/qilat/internal/exchange"
	"github.com/erizeta/qilat/internal/httpfields"
)

// stream is one request/response. Fields under "guarded by c.mu" are shared with the connection reader;
// the rest belong to the handler goroutine.
type stream struct {
	c  *Conn
	id uint32
	x  exchange.Exchange

	body     body
	sink     sink
	waiter   *waiter
	priority atomic.Uint32 // urgency<<1 | incremental, read by the write scheduler

	// guarded by c.mu
	remoteClosed bool  // the client sent END_STREAM
	reset        bool  // RST_STREAM sent or received, or the connection is gone
	counted      bool  // counts toward MaxConcurrentStreams
	sendWindow   int64 // how much DATA the client lets us send
	recvWindow   int64 // how much DATA we let the client send
	recvUnacked  int64 // consumed by the handler but not yet returned with WINDOW_UPDATE
	declared     int64 // content-length, or -1
	received     int64 // DATA payload bytes received
	timer        *time.Timer

	expectContinue bool
}

func newStream(c *Conn, id uint32, x exchange.Exchange) *stream {
	st := &stream{c: c, id: id, x: x, declared: -1}
	st.body.st = st
	st.body.cond.L = &st.body.mu
	st.sink.st = st
	st.priority.Store(httpfields.DefaultUrgency << 1)
	st.waiter = newWaiter(id, &st.priority)
	return st
}

func (st *stream) setPriority(urgency uint32, incremental bool) {
	p := urgency << 1
	if incremental {
		p |= 1
	}
	st.priority.Store(p)
}

const chunkSize = 16 << 10

var chunkPool = sync.Pool{New: func() any { b := make([]byte, 0, chunkSize); return &b }}

// body buffers request DATA until the handler reads it. Flow control bounds how much it can hold.
type body struct {
	st   *stream
	mu   sync.Mutex
	cond sync.Cond

	chunks []*[]byte
	off    int // read offset in chunks[0]
	eof    bool
	err    error
	tooBig bool
	limit  int64
	total  int64
}

// write appends DATA received for the stream. It reports false if the body limit was exceeded,
// in which case the data is dropped.
func (b *body) write(p []byte) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.err != nil || b.tooBig {
		return false
	}
	b.total += int64(len(p))
	if b.limit >= 0 && b.total > b.limit {
		b.tooBig = true
		b.err = exchange.ErrBodyTooLarge
		b.cond.Broadcast()
		return false
	}
	for len(p) > 0 {
		if len(b.chunks) == 0 || len(*b.chunks[len(b.chunks)-1]) == chunkSize {
			b.chunks = append(b.chunks, chunkPool.Get().(*[]byte))
		}
		last := b.chunks[len(b.chunks)-1]
		n := min(len(p), chunkSize-len(*last))
		*last = append(*last, p[:n]...)
		p = p[n:]
	}
	b.cond.Broadcast()
	return true
}

func (b *body) closeWithEOF() {
	b.mu.Lock()
	b.eof = true
	b.cond.Broadcast()
	b.mu.Unlock()
}

func (b *body) fail(err error) {
	b.mu.Lock()
	if b.err == nil {
		b.err = err
	}
	b.cond.Broadcast()
	b.mu.Unlock()
}

func (b *body) Read(p []byte) (int, error) {
	st := b.st
	if st.expectContinue {
		st.expectContinue = false
		st.sink.sendContinue()
	}
	b.mu.Lock()
	for len(b.chunks) == 0 && b.err == nil && !b.eof {
		b.cond.Wait()
	}
	// Data received before the body limit is still delivered; a reset discards it.
	if b.err != nil && !b.tooBig || len(b.chunks) == 0 {
		err := b.err
		if err == nil {
			err = io.EOF
		}
		b.mu.Unlock()
		return 0, err
	}
	n := 0
	for n < len(p) && len(b.chunks) > 0 {
		c := *b.chunks[0]
		m := copy(p[n:], c[b.off:])
		n += m
		b.off += m
		if b.off == len(c) {
			*b.chunks[0] = c[:0]
			chunkPool.Put(b.chunks[0])
			b.chunks[0] = nil
			b.chunks = b.chunks[1:]
			b.off = 0
		}
	}
	b.mu.Unlock()
	st.c.consumed(st, int64(n))
	return n, nil
}

// buffered returns how many received bytes the handler has not read, and drops them.
func (b *body) drop() int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	var n int64
	for i, c := range b.chunks {
		n += int64(len(*c))
		*c = (*c)[:0]
		chunkPool.Put(c)
		b.chunks[i] = nil
	}
	n -= int64(b.off)
	b.chunks, b.off = b.chunks[:0], 0
	if b.err == nil {
		b.err = errStreamReset
	}
	b.cond.Broadcast()
	return n
}

// sink writes the response of one stream.
type sink struct {
	st         *stream
	headerSent bool
	ended      bool // END_STREAM sent
}

var (
	_ exchange.ResponseSink      = (*sink)(nil)
	_ exchange.InformationalSink = (*sink)(nil)
	_ exchange.TrailerSink       = (*sink)(nil)
	_ exchange.CompleteSink      = (*sink)(nil)
)

func (s *sink) WriteHeader(status int, h exchange.HeaderList, _ int64) error {
	s.headerSent = true
	return s.st.c.writeHeaders(s.st, status, h, false, false)
}

func (s *sink) Write(p []byte) (int, error) {
	if err := s.st.c.writeData(s.st, p, false); err != nil {
		return 0, err
	}
	return len(p), nil
}

func (s *sink) Flush() error { return s.st.c.flush(s.st.waiter) }

func (s *sink) Finish() error {
	s.ended = true
	return s.st.c.writeData(s.st, nil, true)
}

func (s *sink) FinishTrailer(trailer exchange.HeaderList) error {
	s.ended = true
	return s.st.c.writeHeaders(s.st, 0, trailer, true, true)
}

func (s *sink) WriteComplete(status int, h exchange.HeaderList, _ int64, p []byte, trailer exchange.HeaderList) error {
	s.headerSent = true
	s.ended = true
	hasTrailer := trailer != nil && trailer.Len() > 0
	if len(p) == 0 && !hasTrailer {
		return s.st.c.writeHeaders(s.st, status, h, true, true)
	}
	if err := s.st.c.writeHeaders(s.st, status, h, false, false); err != nil {
		return err
	}
	if len(p) > 0 {
		if err := s.st.c.writeData(s.st, p, !hasTrailer); err != nil {
			return err
		}
	}
	if hasTrailer {
		return s.st.c.writeHeaders(s.st, 0, trailer, true, true)
	}
	return nil
}

func (s *sink) WriteInformational(status int, h exchange.HeaderList) error {
	if s.headerSent {
		return nil
	}
	if status == 100 {
		s.st.expectContinue = false
	}
	return s.st.c.writeHeaders(s.st, status, h, false, true)
}

// sendContinue answers "Expect: 100-continue" the first time the handler reads the body.
func (s *sink) sendContinue() {
	if !s.headerSent {
		_ = s.st.c.writeHeaders(s.st, 100, nil, false, true)
	}
}
