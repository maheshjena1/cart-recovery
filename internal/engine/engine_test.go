package engine

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	"cartrecovery/internal/clock"
	"cartrecovery/internal/config"
	"cartrecovery/internal/events"
	"cartrecovery/internal/store"
)

var t0 = time.Date(2026, 1, 5, 0, 0, 0, 0, time.UTC)

type fixture struct {
	t   *testing.T
	clk *clock.Fake
	st  *store.Store
	eng *Engine
	n   int
}

func newFixture(t *testing.T) *fixture {
	clk, st := clock.NewFake(t0), store.New()
	return &fixture{t: t, clk: clk, st: st, eng: New(config.Default(), clk, st)}
}

// event ingests typ for cart A at the current fake time.
func (f *fixture) event(typ events.Type) {
	f.t.Helper()
	f.n++
	ev := events.Event{ID: fmt.Sprintf("e%d", f.n), CartID: "A", UserID: "u1", Type: typ}
	if err := f.eng.HandleEvent(ev, f.clk.Now()); err != nil {
		f.t.Fatal(err)
	}
}

// fireDue moves the clock forward by d, then plays the scheduler's part:
// every due timer is handled, then deleted.
func (f *fixture) fireDue(d time.Duration) {
	f.t.Helper()
	f.clk.Advance(d)
	for {
		timer, ok := f.st.NextTimer()
		if !ok || timer.Due.After(f.clk.Now()) {
			return
		}
		if err := f.eng.HandleTimer(timer); err != nil {
			f.t.Fatal(err)
		}
		f.st.DeleteTimer(timer.ID)
	}
}

func (f *fixture) cart() store.Cart {
	c, _ := f.st.Cart("A")
	return c
}

// drainTimers removes and returns every timer, in firing order.
func (f *fixture) drainTimers() []store.Timer {
	var out []store.Timer
	for {
		timer, ok := f.st.NextTimer()
		if !ok {
			return out
		}
		out = append(out, timer)
		f.st.DeleteTimer(timer.ID)
	}
}

func TestTransitions(t *testing.T) {
	// idle is a pseudo-step: a full inactivity window passes.
	const idle = events.Type("(idle 30m)")

	tests := []struct {
		name       string
		steps      []events.Type
		wantStatus store.Status
		wantGen    uint64
	}{
		{"add opens the cart", []events.Type{events.ItemAdded}, store.Active, 1},
		{"every edit bumps the generation", []events.Type{events.ItemAdded, events.ItemUpdated, events.ItemRemoved}, store.Active, 3},
		{"a full idle window abandons, without a bump", []events.Type{events.ItemAdded, idle}, store.Abandoned, 1},
		{"an edit revives an abandoned cart", []events.Type{events.ItemAdded, idle, events.ItemUpdated}, store.Active, 2},
		{"a view revives an abandoned cart", []events.Type{events.ItemAdded, idle, events.CartResumed}, store.Active, 2},
		{"checkout started is activity, not an outcome", []events.Type{events.ItemAdded, events.CheckoutStarted}, store.Active, 2},
		{"order placed is terminal", []events.Type{events.ItemAdded, events.OrderPlaced}, store.Purchased, 2},
		{"events after purchase are ignored", []events.Type{events.ItemAdded, events.OrderPlaced, events.ItemAdded}, store.Purchased, 2},
		{"clear stops the cycle", []events.Type{events.ItemAdded, events.CartCleared}, store.Cleared, 2},
		{"a view does not reopen a cleared cart", []events.Type{events.ItemAdded, events.CartCleared, events.CartResumed}, store.Cleared, 2},
		{"an add reopens a cleared cart", []events.Type{events.ItemAdded, events.CartCleared, events.ItemAdded}, store.Active, 3},
		{"a view of an unknown cart creates nothing", []events.Type{events.CartResumed}, "", 0},
		{"an order for an unknown cart creates nothing", []events.Type{events.OrderPlaced}, "", 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFixture(t)
			for _, step := range tt.steps {
				if step == idle {
					f.fireDue(30 * time.Minute)
				} else {
					f.event(step)
				}
			}
			c := f.cart()
			if c.Status != tt.wantStatus || c.Generation != tt.wantGen {
				t.Fatalf("got status=%q gen=%d, want status=%q gen=%d", c.Status, c.Generation, tt.wantStatus, tt.wantGen)
			}
		})
	}
}

func TestEditResetsClockWithOneTimerPerCart(t *testing.T) {
	f := newFixture(t)
	f.event(events.ItemAdded)
	f.clk.Advance(20 * time.Minute)
	f.event(events.ItemUpdated)

	f.fireDue(10 * time.Minute) // 00:30: the original timer fires, 10m after the edit
	c := f.cart()
	if c.Status != store.Active {
		t.Fatalf("status at 00:30 = %s, want ACTIVE", c.Status)
	}
	if want := t0.Add(50 * time.Minute); !c.CheckDue.Equal(want) {
		t.Fatalf("re-armed for +%v, want +50m", c.CheckDue.Sub(t0))
	}

	f.fireDue(20 * time.Minute) // 00:50
	c = f.cart()
	if c.Status != store.Abandoned || !c.AbandonedAt.Equal(t0.Add(50*time.Minute)) {
		t.Fatalf("at 00:50 got status=%s abandonedAt=+%v, want ABANDONED at +50m", c.Status, c.AbandonedAt.Sub(t0))
	}
}

func TestAbandonmentSchedulesRemindersFromAbandonmentTime(t *testing.T) {
	f := newFixture(t)
	f.event(events.ItemAdded)
	f.fireDue(30 * time.Minute)

	timers := f.drainTimers()
	want := []time.Duration{60 * time.Minute, 90 * time.Minute, 24*time.Hour + 30*time.Minute}
	if len(timers) != len(want) {
		t.Fatalf("got %d reminder timers, want %d", len(timers), len(want))
	}
	for i, timer := range timers {
		if timer.Kind != store.Reminder || timer.OffsetIndex != i || timer.Generation != 1 || !timer.Due.Equal(t0.Add(want[i])) {
			t.Errorf("timer %d = %+v, want reminder gen=1 due at +%v", i, timer, want[i])
		}
	}
}

func TestReminderForAnOldGenerationIsANoOp(t *testing.T) {
	cancelling := []events.Type{events.ItemUpdated, events.CartResumed, events.CartCleared, events.OrderPlaced}
	for _, typ := range cancelling {
		t.Run(string(typ), func(t *testing.T) {
			f := newFixture(t)
			f.event(events.ItemAdded)
			f.fireDue(30 * time.Minute) // abandoned at generation 1
			f.event(typ)                // generation 2

			f.fireDue(48 * time.Hour) // every generation-1 reminder fires
			for i := range config.Default().Offsets {
				if tr, ok := f.st.Trigger(TriggerKey("A", 1, i)); ok {
					t.Fatalf("trigger enqueued for the cancelled cycle: %+v", tr)
				}
			}
		})
	}
}

func TestDuplicateTimerFiringEnqueuesOnce(t *testing.T) {
	f := newFixture(t)
	f.event(events.ItemAdded)
	check, _ := f.st.NextTimer()
	f.fireDue(30 * time.Minute) // abandoned
	reminder, _ := f.st.NextTimer()

	f.clk.Advance(30 * time.Minute)
	if err := f.eng.HandleTimer(reminder); err != nil {
		t.Fatal(err)
	}
	if got := f.st.PendingTriggers(f.clk.Now()); len(got) != 1 || got[0].Key != "A:1:0" {
		t.Fatalf("outbox = %v, want exactly A:1:0", got)
	}

	// The reminder is delivered; then the timer store redelivers the timer.
	// It must not put the reminder back in the outbox.
	sent := f.st.PendingTriggers(f.clk.Now())[0]
	sent.Status = store.Sent
	_ = f.st.Update(func(tx *store.Tx) error { tx.PutTrigger(sent); return nil })
	if err := f.eng.HandleTimer(reminder); err != nil {
		t.Fatal(err)
	}
	if got, _ := f.st.Trigger("A:1:0"); got.Status != store.Sent {
		t.Fatalf("redelivered timer reset the trigger to %s", got.Status)
	}

	// A redelivered inactivity timer must not start a second cycle either.
	before := f.cart()
	if err := f.eng.HandleTimer(check); err != nil {
		t.Fatal(err)
	}
	if f.cart() != before || len(f.st.Journal()) != 1 {
		t.Fatalf("stale inactivity timer changed state: %+v, journal %v", f.cart(), f.st.Journal())
	}
}

func TestDuplicateEventIsIgnored(t *testing.T) {
	f := newFixture(t)
	ev := events.Event{ID: "e1", CartID: "A", UserID: "u1", Type: events.ItemAdded}
	for _, at := range []time.Time{t0, t0.Add(20 * time.Minute)} { // redelivered later
		if err := f.eng.HandleEvent(ev, at); err != nil {
			t.Fatal(err)
		}
	}
	c := f.cart()
	if c.Generation != 1 || !c.LastActivity.Equal(t0) {
		t.Fatalf("duplicate event applied: gen=%d lastActivity=+%v", c.Generation, c.LastActivity.Sub(t0))
	}
}

func TestTriggerKey(t *testing.T) {
	if got := TriggerKey("cart-9", 4, 2); got != "cart-9:4:2" {
		t.Fatalf("TriggerKey = %q, want cart-9:4:2", got)
	}
	// Same cart, same offset, new cycle: must be a different reminder.
	if TriggerKey("A", 1, 0) == TriggerKey("A", 2, 0) {
		t.Fatal("key does not distinguish generations")
	}
}

// BenchmarkHandleEvent measures the engine's hot path on one core: dedupe
// check, state transition, and one transactional commit per event, spread
// over 100k live carts. It does not include log or network I/O.
func BenchmarkHandleEvent(b *testing.B) {
	const carts = 100_000
	clk, st := clock.NewFake(t0), store.New()
	eng := New(config.Default(), clk, st)
	cartIDs := make([]string, carts)
	for i := range cartIDs {
		cartIDs[i] = "cart-" + strconv.Itoa(i)
	}

	i := 0
	for b.Loop() {
		ev := events.Event{ID: strconv.Itoa(i), CartID: cartIDs[i%carts], UserID: "u", Type: events.ItemUpdated}
		if err := eng.HandleEvent(ev, t0); err != nil {
			b.Fatal(err)
		}
		i++
	}
}
