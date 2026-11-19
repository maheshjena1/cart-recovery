// Package store is the pipeline's single transactional store: cart state,
// processed-event ids, durable timers, the trigger outbox and an audit
// journal. Keeping them in one store is what lets a state change and its side
// effects (timers, triggers) commit atomically.
//
// This implementation is in memory. Production maps each map below to a table
// in one database, partitioned by cart id (DESIGN.md, section 10).
package store

import (
	"cmp"
	"container/heap"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"
)

type Status string

const (
	Active    Status = "ACTIVE"    // has items, inactivity clock running
	Abandoned Status = "ABANDONED" // inactive for a full window, reminders scheduled
	Purchased Status = "PURCHASED" // terminal
	Cleared   Status = "CLEARED"   // emptied; an item event reopens it
)

type Cart struct {
	ID        string
	UserID    string
	SessionID string
	Status    Status
	// Generation increments on every shopper event that changes the cart.
	// Timers and triggers carry the generation they were created under and
	// are void once it moves on.
	Generation   uint64
	LastActivity time.Time // server ingest time of the last activity event
	AbandonedAt  time.Time // LastActivity + window; anchor for reminder offsets
	// CheckDue is the due time of the cart's one armed inactivity timer, or
	// zero if none is armed.
	CheckDue time.Time
}

// ShopperID is the id the shopper is addressed and bucketed by: the user id
// when signed in, otherwise the guest session id.
func (c Cart) ShopperID() string {
	if c.UserID != "" {
		return c.UserID
	}
	return c.SessionID
}

type TimerKind string

const (
	Inactivity TimerKind = "inactivity" // has the cart been idle for a full window?
	Reminder   TimerKind = "reminder"   // a reminder offset is due
	Retry      TimerKind = "retry"      // a failed delivery is due for another attempt
)

// Timer is a durable timer row. The ID is deterministic, so adding the same
// timer twice is a no-op.
type Timer struct {
	ID          string
	Kind        TimerKind
	Due         time.Time
	CartID      string
	Generation  uint64        // Reminder only
	OffsetIndex int           // Reminder only
	Offset      time.Duration // Reminder only
}

type TriggerStatus string

const (
	Pending    TriggerStatus = "PENDING"    // in the outbox, waiting for delivery
	Sent       TriggerStatus = "SENT"       // accepted by the notification service
	Cancelled  TriggerStatus = "CANCELLED"  // pre-send check found the cart no longer abandoned
	Suppressed TriggerStatus = "SUPPRESSED" // deliberately not sent: holdout or stale
	Dead       TriggerStatus = "DEAD"       // dead-lettered; needs Replay
)

// Trigger is an outbox row: one reminder for one cart generation. Its Key is
// the idempotency key, so a reminder can be enqueued at most once.
type Trigger struct {
	Key         string
	CartID      string
	ShopperID   string
	Generation  uint64
	OffsetIndex int
	Offset      time.Duration
	DueAt       time.Time
	Status      TriggerStatus
	Reason      string    // why Cancelled, Suppressed or Dead
	Attempts    int       // failed delivery attempts so far
	NextAttempt time.Time // earliest time of the next attempt
}

// Journal entry kinds.
const (
	KindAbandoned    = "Abandoned"
	KindTrigger      = "Trigger"
	KindCancelled    = "Cancelled"
	KindSuppressed   = "Suppressed"
	KindRetry        = "Retry"
	KindDeadLettered = "DeadLettered"
	KindReplayed     = "Replayed"
	KindConflict     = "IdentityConflict" // an event named a different user than the cart's
)

// Entry is one line of the audit journal: an outcome the business cares
// about, written in the same transaction as the state change that caused it.
// In production this is the feed for metrics and experiment analysis.
type Entry struct {
	At         time.Time
	Kind       string
	CartID     string
	Generation uint64
	Offset     time.Duration // not set for KindAbandoned or KindConflict
	Detail     string
}

func (e Entry) String() string {
	switch e.Kind {
	case KindAbandoned:
		return fmt.Sprintf("Abandoned(%s, gen=%d)", e.CartID, e.Generation)
	case KindConflict:
		return fmt.Sprintf("%s(%s, %s)", e.Kind, e.CartID, e.Detail)
	}
	s := fmt.Sprintf("%s(%s, offset=%s", e.Kind, e.CartID, shortDuration(e.Offset))
	if e.Detail != "" {
		s += ", " + e.Detail
	}
	return s + ")"
}

// shortDuration prints 30m and 1h where time.Duration prints 30m0s and 1h0m0s.
func shortDuration(d time.Duration) string {
	s := d.String()
	if strings.HasSuffix(s, "m0s") {
		s = strings.TrimSuffix(s, "0s")
	}
	if strings.HasSuffix(s, "h0m") {
		s = strings.TrimSuffix(s, "0m")
	}
	return s
}

type Store struct {
	// Simplification: one global lock serialises every transaction.
	// Production relies on a single writer per partition plus database
	// transactions instead.
	mu       sync.Mutex
	carts    map[string]Cart
	seen     map[string]struct{} // processed event ids
	timers   map[string]Timer
	due      dueHeap             // index over timers by (Due, ID); may hold stale entries
	triggers map[string]Trigger  // the outbox, keyed by idempotency key
	pending  map[string]struct{} // index over triggers with Status == Pending
	journal  []Entry
}

func New() *Store {
	return &Store{
		carts:    make(map[string]Cart),
		seen:     make(map[string]struct{}),
		timers:   make(map[string]Timer),
		triggers: make(map[string]Trigger),
		pending:  make(map[string]struct{}),
	}
}

// Update runs fn as one transaction. Writes made through tx are buffered and
// applied together only if fn returns nil; on error nothing changes. Reads
// through tx see the state as of the start of the transaction.
func (s *Store) Update(fn func(tx *Tx) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	tx := &Tx{s: s}
	if err := fn(tx); err != nil {
		return err
	}
	for _, write := range tx.writes {
		write()
	}
	return nil
}

// Tx is a transaction handle, valid only inside Update.
type Tx struct {
	s      *Store
	writes []func()
}

func (tx *Tx) Cart(id string) (Cart, bool) {
	c, ok := tx.s.carts[id]
	return c, ok
}

func (tx *Tx) EventSeen(eventID string) bool {
	_, ok := tx.s.seen[eventID]
	return ok
}

func (tx *Tx) TriggerExists(key string) bool {
	_, ok := tx.s.triggers[key]
	return ok
}

func (tx *Tx) PutCart(c Cart) {
	tx.writes = append(tx.writes, func() { tx.s.carts[c.ID] = c })
}

func (tx *Tx) MarkEventSeen(eventID string) {
	tx.writes = append(tx.writes, func() { tx.s.seen[eventID] = struct{}{} })
}

// AddTimer inserts t. A timer with the same ID already present is kept as is.
func (tx *Tx) AddTimer(t Timer) {
	tx.writes = append(tx.writes, func() {
		if _, ok := tx.s.timers[t.ID]; ok {
			return
		}
		tx.s.timers[t.ID] = t
		heap.Push(&tx.s.due, dueEntry{t.Due, t.ID})
	})
}

func (tx *Tx) PutTrigger(t Trigger) {
	tx.writes = append(tx.writes, func() {
		tx.s.triggers[t.Key] = t
		if t.Status == Pending {
			tx.s.pending[t.Key] = struct{}{}
		} else {
			delete(tx.s.pending, t.Key)
		}
	})
}

func (tx *Tx) Record(e Entry) {
	tx.writes = append(tx.writes, func() { tx.s.journal = append(tx.s.journal, e) })
}

func (s *Store) Cart(id string) (Cart, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	c, ok := s.carts[id]
	return c, ok
}

func (s *Store) Trigger(key string) (Trigger, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.triggers[key]
	return t, ok
}

// NextTimer returns the timer with the earliest due time, ties broken by ID.
// It does not remove it: the caller deletes the timer after handling it, so a
// crash in between means the timer fires again (at-least-once).
func (s *Store) NextTimer() (Timer, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for len(s.due) > 0 {
		e := s.due[0]
		// An entry is live only if the timer still exists with that due
		// time. Otherwise the timer was deleted, and its ID may since have
		// been reused with a different due time: that timer has an entry of
		// its own further down.
		if t, ok := s.timers[e.id]; ok && t.Due.Equal(e.due) {
			return t, true
		}
		heap.Pop(&s.due)
	}
	return Timer{}, false
}

func (s *Store) DeleteTimer(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.timers, id)
}

// PendingTriggers returns the outbox rows ready for a delivery attempt at
// now, ordered by due time and then key.
func (s *Store) PendingTriggers(now time.Time) []Trigger {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []Trigger
	for key := range s.pending {
		if t := s.triggers[key]; !t.NextAttempt.After(now) {
			out = append(out, t)
		}
	}
	sortTriggers(out)
	return out
}

// DeadLetters returns the dead-lettered triggers, each with its Reason.
func (s *Store) DeadLetters() []Trigger {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []Trigger
	for _, t := range s.triggers {
		if t.Status == Dead {
			out = append(out, t)
		}
	}
	sortTriggers(out)
	return out
}

func sortTriggers(ts []Trigger) {
	slices.SortFunc(ts, func(a, b Trigger) int {
		return cmp.Or(a.DueAt.Compare(b.DueAt), cmp.Compare(a.Key, b.Key))
	})
}

// Journal returns a copy of the audit journal in commit order.
func (s *Store) Journal() []Entry {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.journal)
}

type dueEntry struct {
	due time.Time
	id  string
}

type dueHeap []dueEntry

func (h dueHeap) Len() int { return len(h) }
func (h dueHeap) Less(i, j int) bool {
	if !h[i].due.Equal(h[j].due) {
		return h[i].due.Before(h[j].due)
	}
	return h[i].id < h[j].id
}
func (h dueHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *dueHeap) Push(x any)   { *h = append(*h, x.(dueEntry)) }
func (h *dueHeap) Pop() any {
	old := *h
	e := old[len(old)-1]
	*h = old[:len(old)-1]
	return e
}
