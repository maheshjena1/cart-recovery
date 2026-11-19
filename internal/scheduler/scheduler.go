// Package scheduler fires durable timers. The timers live in the store; the
// scheduler holds no state of its own beyond one wake-up on the clock, so a
// restarted scheduler picks up exactly where the old one stopped.
package scheduler

import (
	"log/slog"
	"sync"
	"time"

	"cartrecovery/internal/clock"
	"cartrecovery/internal/store"
)

// retryDelay is how long to wait before retrying a timer whose handler
// failed.
const retryDelay = time.Second

type Scheduler struct {
	clk  clock.Clock
	st   *store.Store
	fire func(store.Timer) error

	mu        sync.Mutex
	wake      clock.Timer // the one pending wake-up, or nil
	wakeAt    time.Time
	holdUntil time.Time // no wake-up before this, after a handler failure
	stopped   bool
}

// New returns a scheduler that calls fire for each due timer, in order of due
// time and then timer ID. Delivery is at least once: a timer is deleted only
// after fire returns nil, so fire must be idempotent.
func New(clk clock.Clock, st *store.Store, fire func(store.Timer) error) *Scheduler {
	return &Scheduler{clk: clk, st: st, fire: fire}
}

// Arm makes sure a wake-up is set for the earliest timer in the store. Call
// it after adding timers, and once at start-up.
func (s *Scheduler) Arm() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.arm()
}

// Stop cancels the pending wake-up. Timers stay in the store.
func (s *Scheduler) Stop() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stopped = true
	if s.wake != nil {
		s.wake.Stop()
		s.wake = nil
	}
}

func (s *Scheduler) arm() {
	if s.stopped {
		return
	}
	next, ok := s.st.NextTimer()
	if !ok {
		return
	}
	at := next.Due
	if at.Before(s.holdUntil) {
		at = s.holdUntil
	}
	if s.wake != nil {
		if !at.Before(s.wakeAt) {
			return // the pending wake-up is early enough
		}
		s.wake.Stop()
	}
	s.wakeAt = at
	s.wake = s.clk.AfterFunc(at.Sub(s.clk.Now()), s.tick)
}

func (s *Scheduler) tick() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopped {
		return
	}
	s.wake = nil
	for {
		now := s.clk.Now()
		t, ok := s.st.NextTimer()
		if !ok || t.Due.After(now) {
			break
		}
		if err := s.fire(t); err != nil {
			// Leave the timer in place and come back later. Timers behind
			// it wait too, which keeps firing order intact.
			slog.Error("timer handler failed; will retry", "timer", t.ID, "err", err)
			s.holdUntil = now.Add(retryDelay)
			break
		}
		s.st.DeleteTimer(t.ID)
	}
	s.arm()
}
