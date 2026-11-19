package delivery

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"cartrecovery/internal/clock"
	"cartrecovery/internal/config"
	"cartrecovery/internal/notifier"
	"cartrecovery/internal/store"
)

var t0 = time.Date(2026, 1, 5, 0, 0, 0, 0, time.UTC)

const key = "A:1:0"

type fixture struct {
	clk    *clock.Fake
	st     *store.Store
	n      *notifier.Stub
	orders *notifier.Orders
	relay  *Relay
}

// newFixture has cart A abandoned at generation 1 and its first reminder in
// the outbox, due now.
func newFixture(cfg config.Config) *fixture {
	f := &fixture{clk: clock.NewFake(t0), st: store.New(), n: notifier.NewStub(), orders: notifier.NewOrders()}
	f.relay = New(cfg, f.clk, f.st, f.n, f.orders, func() float64 { return 0.5 })
	_ = f.st.Update(func(tx *store.Tx) error {
		tx.PutCart(store.Cart{ID: "A", UserID: "u1", Status: store.Abandoned, Generation: 1})
		tx.PutTrigger(store.Trigger{Key: key, CartID: "A", ShopperID: "u1", Generation: 1, Offset: 30 * time.Minute, DueAt: t0, Status: store.Pending})
		return nil
	})
	return f
}

func (f *fixture) drain(t *testing.T) {
	t.Helper()
	if err := f.relay.Drain(); err != nil {
		t.Fatal(err)
	}
}

func (f *fixture) wantStatus(t *testing.T, status store.TriggerStatus, reason string) {
	t.Helper()
	if tr, _ := f.st.Trigger(key); tr.Status != status || tr.Reason != reason {
		t.Fatalf("trigger is %s (%q), want %s (%q)", tr.Status, tr.Reason, status, reason)
	}
}

func TestBackoff(t *testing.T) {
	const base, max = time.Second, 30 * time.Second
	tests := []struct {
		attempt int
		jitter  float64
		want    time.Duration
	}{
		{1, 0, 500 * time.Millisecond}, // lower bound: half the nominal delay
		{1, 0.5, 750 * time.Millisecond},
		{2, 0, time.Second},
		{3, 0, 2 * time.Second},
		{5, 0, 8 * time.Second},
		{6, 0, 15 * time.Second},  // nominal 32s, capped at 30s
		{60, 0, 15 * time.Second}, // no overflow far past the cap
	}
	for _, tt := range tests {
		if got := Backoff(tt.attempt, base, max, tt.jitter); got != tt.want {
			t.Errorf("Backoff(attempt=%d, jitter=%v) = %v, want %v", tt.attempt, tt.jitter, got, tt.want)
		}
	}
	// Jitter stays below the nominal delay.
	if got := Backoff(3, base, max, 0.999999); got < 2*time.Second || got >= 4*time.Second {
		t.Errorf("Backoff(3) with max jitter = %v, want within [2s, 4s)", got)
	}
}

func TestInHoldoutSplit(t *testing.T) {
	const n = 10_000
	in, moved := 0, 0
	for i := range n {
		id := fmt.Sprintf("user-%d", i)
		if InHoldout(id, "salt", 10) {
			in++
		}
		if InHoldout(id, "salt", 50) != InHoldout(id, "other-salt", 50) {
			moved++
		}
	}
	if in < n*8/100 || in > n*12/100 {
		t.Fatalf("%d of %d shoppers in a 10%% holdout", in, n)
	}
	// A different salt must reshuffle shoppers, or two experiments would
	// hold out the same people. Independent 50% splits disagree on about half.
	if moved < n*4/10 || moved > n*6/10 {
		t.Fatalf("changing the salt moved %d of %d shoppers, want about half", moved, n)
	}
	if InHoldout("anyone", "salt", 0) || !InHoldout("anyone", "salt", 100) {
		t.Fatal("0% must hold out nobody and 100% everybody")
	}
}

func TestSendsOnce(t *testing.T) {
	f := newFixture(config.Default())
	f.drain(t)
	f.drain(t)
	f.wantStatus(t, store.Sent, "")
	if got := f.n.Attempts(); got != 1 {
		t.Fatalf("notifier called %d times, want 1", got)
	}
}

// The response to a successful send is lost. The relay cannot tell this from
// a failure, so it sends again; the idempotency key makes that harmless.
func TestLostAckIsRedeliveredAndDroppedByKey(t *testing.T) {
	f := newFixture(config.Default())
	f.n.LoseAckNext(1)

	f.drain(t)
	f.clk.Advance(time.Minute)
	f.drain(t)

	f.wantStatus(t, store.Sent, "")
	if sent, dups := len(f.n.Sent()), f.n.Duplicates(); sent != 1 || dups != 1 {
		t.Fatalf("notifier accepted %d and dropped %d, want 1 and 1", sent, dups)
	}
}

func TestAttemptCapDeadLetters(t *testing.T) {
	cfg := config.Default()
	cfg.MaxAttempts = 3
	f := newFixture(cfg)
	f.n.FailNext(100, notifier.ErrTransient)

	for range 10 {
		f.drain(t)
		f.clk.Advance(time.Minute)
	}

	f.wantStatus(t, store.Dead, "gave up after 3 attempts: transient failure")
	if got := f.n.Attempts(); got != 3 {
		t.Fatalf("notifier called %d times, want 3", got)
	}
	if dlq := f.st.DeadLetters(); len(dlq) != 1 || dlq[0].Key != key {
		t.Fatalf("dead-letter queue = %v, want %s", dlq, key)
	}
}

func TestRetryWaitsForBackoff(t *testing.T) {
	f := newFixture(config.Default())
	f.n.FailNext(1, notifier.ErrTransient)
	f.drain(t)

	f.clk.Advance(700 * time.Millisecond) // backoff is 750ms at jitter 0.5
	f.drain(t)
	if got := f.n.Attempts(); got != 1 {
		t.Fatalf("retried after 700ms: %d attempts", got)
	}
	f.clk.Advance(50 * time.Millisecond)
	f.drain(t)
	f.wantStatus(t, store.Sent, "")
}

func TestOrderServiceDownFailsClosed(t *testing.T) {
	f := newFixture(config.Default())
	f.orders.SetError(errors.New("timeout"))
	f.drain(t)

	if got := f.n.Attempts(); got != 0 {
		t.Fatal("sent without confirming the cart is unpurchased")
	}
	if tr, _ := f.st.Trigger(key); tr.Status != store.Pending || tr.Attempts != 1 {
		t.Fatalf("trigger = %s attempts=%d, want PENDING with 1 attempt", tr.Status, tr.Attempts)
	}

	f.orders.SetError(nil)
	f.clk.Advance(time.Minute)
	f.drain(t)
	f.wantStatus(t, store.Sent, "")
}

// orderServiceDownFor runs the relay once a second for d with the order
// service down, then brings it back.
func (f *fixture) orderServiceDownFor(t *testing.T, d time.Duration) {
	t.Helper()
	f.orders.SetError(errors.New("timeout"))
	for elapsed := time.Duration(0); elapsed < d; elapsed += time.Second {
		f.drain(t)
		f.clk.Advance(time.Second)
	}
	f.orders.SetError(nil)
}

func TestRetriesRideOutAShortOutage(t *testing.T) {
	f := newFixture(config.Default())
	f.orderServiceDownFor(t, 2*time.Minute)

	if tr, _ := f.st.Trigger(key); tr.Status != store.Pending {
		t.Fatalf("trigger is %s (%q) after a 2m outage, want PENDING", tr.Status, tr.Reason)
	}
	for range 10 * 60 { // keep draining; the next retry is a few minutes out
		f.drain(t)
		f.clk.Advance(time.Second)
	}
	f.wantStatus(t, store.Sent, "")
	if len(f.st.DeadLetters()) != 0 {
		t.Fatal("outage within the retry budget produced a dead letter")
	}
}

func TestRetriesStopAtTheStalenessBound(t *testing.T) {
	cfg := config.Default()
	cfg.MaxAttempts = 1000 // out of the way: the staleness bound must be what stops retries
	f := newFixture(cfg)
	f.orderServiceDownFor(t, 40*time.Minute)

	tr, _ := f.st.Trigger(key)
	if tr.Status != store.Dead || !strings.Contains(tr.Reason, "staleness bound") {
		t.Fatalf("trigger is %s (%q), want DEAD at the staleness bound", tr.Status, tr.Reason)
	}
	if tr.Attempts <= 5 {
		t.Fatalf("gave up after %d attempts; the bound, not the attempt cap, should decide, and later than this", tr.Attempts)
	}
	if last := tr.NextAttempt; last.After(t0.Add(cfg.MaxLateness)) {
		t.Fatalf("scheduled an attempt at +%v, past the bound", last.Sub(t0))
	}
	if f.n.Attempts() != 0 {
		t.Fatal("sent while the order service was down")
	}

	// Replayed now, past the bound, it is suppressed, never sent.
	if err := f.relay.Replay(key); err != nil {
		t.Fatal(err)
	}
	f.wantStatus(t, store.Suppressed, "stale")
}

func TestPreSendChecks(t *testing.T) {
	tests := []struct {
		name       string
		arrange    func(f *fixture)
		wantStatus store.TriggerStatus
		wantReason string
	}{
		{"order placed, event not yet consumed", func(f *fixture) { f.orders.Place("A") }, store.Cancelled, "order_placed"},
		{"cart edited after the trigger was enqueued", func(f *fixture) {
			_ = f.st.Update(func(tx *store.Tx) error {
				tx.PutCart(store.Cart{ID: "A", UserID: "u1", Status: store.Active, Generation: 2})
				return nil
			})
		}, store.Cancelled, "cart_changed"},
		{"past the staleness bound", func(f *fixture) { f.clk.Advance(31 * time.Minute) }, store.Suppressed, "stale"},
		{"at the staleness bound", func(f *fixture) { f.clk.Advance(30 * time.Minute) }, store.Sent, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFixture(config.Default())
			tt.arrange(f)
			f.drain(t)
			f.wantStatus(t, tt.wantStatus, tt.wantReason)
			if sent := len(f.n.Sent()) == 1; sent != (tt.wantStatus == store.Sent) {
				t.Fatalf("sent = %v", sent)
			}
		})
	}
}

func TestReplayRejectsTriggersNotDeadLettered(t *testing.T) {
	f := newFixture(config.Default())
	if err := f.relay.Replay(key); err == nil {
		t.Fatal("replayed a pending trigger")
	}
	if err := f.relay.Replay("no-such-key"); err == nil {
		t.Fatal("replayed an unknown trigger")
	}
}

// An operator replays the dead-letter queue long after the outage. The
// replay itself is safe: the reminder is too old to be useful and is not sent.
func TestLateReplayIsSuppressedNotSent(t *testing.T) {
	f := newFixture(config.Default())
	f.n.FailNext(1, fmt.Errorf("rejected: %w", notifier.ErrPermanent))
	f.drain(t)
	f.wantStatus(t, store.Dead, "rejected: permanent failure")

	f.clk.Advance(6 * time.Hour)
	if err := f.relay.Replay(key); err != nil {
		t.Fatal(err)
	}
	f.wantStatus(t, store.Suppressed, "stale")
	if got := len(f.n.Sent()); got != 0 {
		t.Fatalf("stale reminder sent on replay")
	}
}
