package clock

import (
	"container/heap"
	"sync"
	"time"
)

// Fake is a manually advanced clock. Time moves only through Advance and Set,
// which run due timers synchronously on the caller's goroutine, ordered by due
// time and then by registration order. A test therefore produces the same
// interleaving on every run.
type Fake struct {
	mu     sync.Mutex
	now    time.Time
	seq    uint64
	timers fakeHeap
}

func NewFake(start time.Time) *Fake { return &Fake{now: start} }

func (c *Fake) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *Fake) AfterFunc(d time.Duration, f func()) Timer {
	c.mu.Lock()
	defer c.mu.Unlock()
	if d < 0 {
		d = 0
	}
	c.seq++
	t := &fakeTimer{clock: c, when: c.now.Add(d), seq: c.seq, f: f}
	heap.Push(&c.timers, t)
	return t
}

// Advance moves the clock forward by d, firing every timer due on the way.
func (c *Fake) Advance(d time.Duration) { c.Set(c.Now().Add(d)) }

// Set moves the clock forward to t, firing every timer due on the way. While a
// timer runs, Now reports that timer's due time. Timers registered by a
// running timer fire in the same call if they are due by t. Set panics if t is
// in the past: a clock that runs backwards is a test bug.
func (c *Fake) Set(t time.Time) {
	for {
		f := c.next(t)
		if f == nil {
			return
		}
		f() // outside the lock, so f may use the clock
	}
}

// next pops the earliest timer due by t and moves the clock to its due time.
// With no such timer it moves the clock to t and returns nil.
func (c *Fake) next(t time.Time) func() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if t.Before(c.now) {
		panic("clock: Set to a time before Now")
	}
	for len(c.timers) > 0 && !c.timers[0].when.After(t) {
		ft := heap.Pop(&c.timers).(*fakeTimer)
		if ft.done {
			continue
		}
		ft.done = true
		if ft.when.After(c.now) {
			c.now = ft.when
		}
		return ft.f
	}
	c.now = t
	return nil
}

type fakeTimer struct {
	clock *Fake
	when  time.Time
	seq   uint64
	f     func()
	done  bool // fired or stopped
}

func (t *fakeTimer) Stop() bool {
	t.clock.mu.Lock()
	defer t.clock.mu.Unlock()
	pending := !t.done
	t.done = true
	return pending
}

type fakeHeap []*fakeTimer

func (h fakeHeap) Len() int { return len(h) }
func (h fakeHeap) Less(i, j int) bool {
	if !h[i].when.Equal(h[j].when) {
		return h[i].when.Before(h[j].when)
	}
	return h[i].seq < h[j].seq
}
func (h fakeHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *fakeHeap) Push(x any)   { *h = append(*h, x.(*fakeTimer)) }
func (h *fakeHeap) Pop() any {
	old := *h
	t := old[len(old)-1]
	*h = old[:len(old)-1]
	return t
}
