package store

import (
	"errors"
	"strings"
	"testing"
	"time"
)

var t0 = time.Date(2026, 1, 5, 0, 0, 0, 0, time.UTC)

func TestUpdateIsAtomic(t *testing.T) {
	s := New()
	boom := errors.New("boom")

	err := s.Update(func(tx *Tx) error {
		tx.PutCart(Cart{ID: "A", Status: Active})
		tx.MarkEventSeen("e1")
		tx.AddTimer(Timer{ID: "t1", Due: t0})
		tx.PutTrigger(Trigger{Key: "A:1:0", Status: Pending})
		tx.Record(Entry{Kind: KindAbandoned, CartID: "A"})
		return boom
	})
	if !errors.Is(err, boom) {
		t.Fatalf("Update error = %v, want boom", err)
	}

	if _, ok := s.Cart("A"); ok {
		t.Error("cart written by a failed transaction")
	}
	if _, ok := s.NextTimer(); ok {
		t.Error("timer written by a failed transaction")
	}
	if got := s.PendingTriggers(t0); len(got) != 0 {
		t.Errorf("trigger written by a failed transaction: %v", got)
	}
	if got := s.Journal(); len(got) != 0 {
		t.Errorf("journal written by a failed transaction: %v", got)
	}
	_ = s.Update(func(tx *Tx) error {
		if tx.EventSeen("e1") {
			t.Error("event marked seen by a failed transaction")
		}
		return nil
	})
}

func TestNextTimerOrdersByDueThenIDAndSkipsDeleted(t *testing.T) {
	s := New()
	_ = s.Update(func(tx *Tx) error {
		tx.AddTimer(Timer{ID: "c", Due: t0.Add(time.Minute)})
		tx.AddTimer(Timer{ID: "b", Due: t0.Add(time.Minute)})
		tx.AddTimer(Timer{ID: "a", Due: t0.Add(time.Hour)})
		tx.AddTimer(Timer{ID: "b", Due: t0}) // same ID: ignored
		return nil
	})

	var got []string
	for {
		timer, ok := s.NextTimer()
		if !ok {
			break
		}
		got = append(got, timer.ID)
		s.DeleteTimer(timer.ID)
	}
	if order := strings.Join(got, " "); order != "b c a" {
		t.Fatalf("order = %q, want %q", order, "b c a")
	}
}

// Retry timer IDs are reused: a dead-lettered trigger that is replayed starts
// again at attempt 1 and gets the same ID with a new due time. The deleted
// timer's leftover heap entry must not drag the new one ahead of its turn.
func TestReusedTimerIDKeepsDueOrder(t *testing.T) {
	s := New()
	add := func(id string, due time.Duration) {
		_ = s.Update(func(tx *Tx) error {
			tx.AddTimer(Timer{ID: id, Due: t0.Add(due)})
			return nil
		})
	}
	add("retry/k/1", time.Minute)
	first, _ := s.NextTimer()
	s.DeleteTimer(first.ID)

	add("retry/k/1", 3*time.Hour) // same ID, much later
	add("other", 2*time.Hour)

	var got []string
	for {
		timer, ok := s.NextTimer()
		if !ok {
			break
		}
		got = append(got, timer.ID)
		s.DeleteTimer(timer.ID)
	}
	if order := strings.Join(got, " "); order != "other retry/k/1" {
		t.Fatalf("order = %q, want %q", order, "other retry/k/1")
	}
}

func TestPendingTriggersHonoursNextAttempt(t *testing.T) {
	s := New()
	_ = s.Update(func(tx *Tx) error {
		tx.PutTrigger(Trigger{Key: "ready", Status: Pending})
		tx.PutTrigger(Trigger{Key: "backing-off", Status: Pending, NextAttempt: t0.Add(time.Minute)})
		tx.PutTrigger(Trigger{Key: "sent", Status: Sent})
		return nil
	})
	got := s.PendingTriggers(t0)
	if len(got) != 1 || got[0].Key != "ready" {
		t.Fatalf("pending at t0 = %v, want only ready", got)
	}
	if got := s.PendingTriggers(t0.Add(time.Minute)); len(got) != 2 {
		t.Fatalf("pending at t0+1m = %v, want 2", got)
	}
}

func TestEntryString(t *testing.T) {
	tests := []struct {
		e    Entry
		want string
	}{
		{Entry{Kind: KindAbandoned, CartID: "A", Generation: 2}, "Abandoned(A, gen=2)"},
		{Entry{Kind: KindTrigger, CartID: "A", Offset: 30 * time.Minute}, "Trigger(A, offset=30m)"},
		{Entry{Kind: KindTrigger, CartID: "A", Offset: 24 * time.Hour}, "Trigger(A, offset=24h)"},
		{Entry{Kind: KindTrigger, CartID: "A", Offset: 90 * time.Second}, "Trigger(A, offset=1m30s)"},
		{Entry{Kind: KindCancelled, CartID: "A", Offset: time.Hour, Detail: "order_placed"}, "Cancelled(A, offset=1h, order_placed)"},
		{Entry{Kind: KindConflict, CartID: "A", Detail: "event e2 names u2, cart belongs to u1"}, "IdentityConflict(A, event e2 names u2, cart belongs to u1)"},
	}
	for _, tt := range tests {
		if got := tt.e.String(); got != tt.want {
			t.Errorf("String() = %q, want %q", got, tt.want)
		}
	}
}
