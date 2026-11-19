package delivery

import (
	"errors"
	"fmt"
	"sync"
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

	// The cap is reached 3 minutes in, well inside the 30-minute bound: a
	// person can still fix this in time, so it is a dead letter.
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
	cfg := config.Default() // under the defaults the bound, not the cap, must stop retries
	f := newFixture(cfg)
	f.orderServiceDownFor(t, 40*time.Minute)

	// Lateness is a metric, not a ticket: suppressed and journalled, and
	// nothing for an operator to replay.
	f.wantStatus(t, store.Suppressed, "retry_deadline")
	tr, _ := f.st.Trigger(key)
	if tr.Attempts < 10 || tr.Attempts >= cfg.MaxAttempts {
		t.Fatalf("stopped after %d attempts, want the bound to decide: well past 10 and under the cap of %d", tr.Attempts, cfg.MaxAttempts)
	}
	if last := tr.NextAttempt; last.After(t0.Add(cfg.MaxLateness)) {
		t.Fatalf("scheduled an attempt at +%v, past the bound", last.Sub(t0))
	}
	if dlq := f.st.DeadLetters(); len(dlq) != 0 {
		t.Fatalf("dead-letter queue = %+v, want empty", dlq)
	}
	if f.n.Attempts() != 0 {
		t.Fatal("sent while the order service was down")
	}
	if err := f.relay.Replay(key); err == nil {
		t.Fatal("a suppressed reminder was accepted for replay")
	}
	journal := f.st.Journal()
	if last := journal[len(journal)-1]; last.Kind != store.KindSuppressed || last.Detail != "retry_deadline" {
		t.Fatalf("last journal entry = %v, want Suppressed(retry_deadline)", last)
	}
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

// slowOrders answers after d has passed: "not purchased", or err if set.
type slowOrders struct {
	clk *clock.Fake
	d   time.Duration
	err error
}

func (o slowOrders) Placed(string) (bool, error) {
	o.clk.Advance(o.d)
	return false, o.err
}

// The reminder is fresh when the pass starts and stale by the time the order
// service answers. The staleness bound must hold at the moment of sending.
func TestSlowOrderCheckCannotPushASendPastTheStalenessBound(t *testing.T) {
	f := newFixture(config.Default())
	f.relay = New(config.Default(), f.clk, f.st, f.n, slowOrders{clk: f.clk, d: 2 * time.Minute}, func() float64 { return 0.5 })
	f.clk.Advance(29 * time.Minute) // 1m inside the 30m bound

	f.drain(t)

	f.wantStatus(t, store.Suppressed, "stale")
	if got := f.n.Attempts(); got != 0 {
		t.Fatalf("sent a reminder %v past its due time", 31*time.Minute)
	}
}

// The order call fails, and by then the reminder has expired. A replay could
// only suppress it, so it must not enter the dead-letter queue.
func TestOrderCheckFailingAfterTheBoundSuppressesNotDeadLetters(t *testing.T) {
	f := newFixture(config.Default())
	slowAndBroken := slowOrders{clk: f.clk, d: 2 * time.Minute, err: errors.New("timeout")}
	f.relay = New(config.Default(), f.clk, f.st, f.n, slowAndBroken, func() float64 { return 0.5 })
	f.clk.Advance(29 * time.Minute)

	f.drain(t)

	f.wantStatus(t, store.Suppressed, "stale")
	if dlq := f.st.DeadLetters(); len(dlq) != 0 {
		t.Fatalf("expired reminder dead-lettered: %+v", dlq)
	}
}

// racingOrders answers "not purchased", but while the call is in flight the
// engine commits a change to the cart.
type racingOrders struct {
	st     *store.Store
	during store.Cart
}

func (o racingOrders) Placed(string) (bool, error) {
	_ = o.st.Update(func(tx *store.Tx) error { tx.PutCart(o.during); return nil })
	return false, nil
}

// A clear, resume or purchase is consumed while the order-service call is in
// flight. The cart looked abandoned before the call and must be looked at
// again before sending.
func TestCartChangeDuringTheOrderCheckCancelsTheSend(t *testing.T) {
	changes := map[string]store.Cart{
		"cleared":   {ID: "A", UserID: "u1", Status: store.Cleared, Generation: 2},
		"resumed":   {ID: "A", UserID: "u1", Status: store.Active, Generation: 2},
		"purchased": {ID: "A", UserID: "u1", Status: store.Purchased, Generation: 2},
	}
	for name, during := range changes {
		t.Run(name, func(t *testing.T) {
			f := newFixture(config.Default())
			f.relay = New(config.Default(), f.clk, f.st, f.n, racingOrders{f.st, during}, func() float64 { return 0.5 })

			f.drain(t)

			f.wantStatus(t, store.Cancelled, "cart_changed")
			if got := f.n.Attempts(); got != 0 {
				t.Fatal("sent a reminder for a cart that changed during the order check")
			}
		})
	}
}

// slowNotifier takes d to answer, then delegates (or fails with err).
type slowNotifier struct {
	clk   *clock.Fake
	d     time.Duration
	err   error
	inner *notifier.Stub
}

func (n slowNotifier) Send(tr notifier.Trigger) error {
	n.clk.Advance(n.d)
	if n.err != nil {
		return n.err
	}
	return n.inner.Send(tr)
}

// The bound decides whether to start the send. A send started inside the
// bound that is accepted after it still counts as sent, and is stamped with
// the time the call returned.
func TestSlowSendStartedInsideTheBoundIsStampedWhenItReturns(t *testing.T) {
	f := newFixture(config.Default())
	f.relay = New(config.Default(), f.clk, f.st, slowNotifier{clk: f.clk, d: 10 * time.Second, inner: f.n}, f.orders, func() float64 { return 0.5 })
	f.clk.Advance(29*time.Minute + 55*time.Second) // 5s inside the bound; the call takes 10s

	f.drain(t)

	f.wantStatus(t, store.Sent, "")
	journal := f.st.Journal()
	if last, want := journal[len(journal)-1].At, t0.Add(30*time.Minute+5*time.Second); !last.Equal(want) {
		t.Fatalf("outcome stamped +%v, want +%v (when the call returned)", last.Sub(t0), want.Sub(t0))
	}
}

// Backoff is measured from the failure, not from before a slow call.
func TestRetryBackoffStartsWhenAFailedSendReturns(t *testing.T) {
	f := newFixture(config.Default())
	slow := slowNotifier{clk: f.clk, d: 10 * time.Second, err: notifier.ErrTransient, inner: f.n}
	f.relay = New(config.Default(), f.clk, f.st, slow, f.orders, func() float64 { return 0.5 })

	f.drain(t)

	tr, _ := f.st.Trigger(key)
	if want := t0.Add(10*time.Second + 750*time.Millisecond); !tr.NextAttempt.Equal(want) {
		t.Fatalf("next attempt at +%v, want +%v", tr.NextAttempt.Sub(t0), want.Sub(t0))
	}
}

// The send fails on the attempt that reaches the cap. Whether that is a dead
// letter depends on when the failure came back: inside the bound a person can
// still act; after it, a replay could only suppress the reminder.
func TestFailureOnTheCapAttemptIsDeadLetteredInsideTheBoundAndSuppressedAfter(t *testing.T) {
	cfg := config.Default()
	cfg.MaxAttempts = 1 // the first failure is the cap attempt
	tests := []struct {
		name       string
		sendTime   time.Duration // how long the failing call takes
		wantStatus store.TriggerStatus
		wantReason string
	}{
		{"failure returns inside the bound", 4 * time.Second, store.Dead, "gave up after 1 attempts: transient failure"},
		{"failure returns after the bound", 10 * time.Second, store.Suppressed, "stale"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFixture(cfg)
			slow := slowNotifier{clk: f.clk, d: tt.sendTime, err: notifier.ErrTransient, inner: f.n}
			f.relay = New(cfg, f.clk, f.st, slow, f.orders, func() float64 { return 0.5 })
			f.clk.Advance(29*time.Minute + 55*time.Second) // 5s inside the bound when the call starts

			f.drain(t)

			f.wantStatus(t, tt.wantStatus, tt.wantReason)
			if dead := len(f.st.DeadLetters()) == 1; dead != (tt.wantStatus == store.Dead) {
				t.Fatalf("dead-letter queue = %+v", f.st.DeadLetters())
			}
		})
	}
}

// barrierNotifier holds every Send until two are in flight, so two relays are
// guaranteed to have both read the row as pending before either finishes it.
type barrierNotifier struct {
	wg    sync.WaitGroup
	inner *notifier.Stub
}

func (b *barrierNotifier) Send(tr notifier.Trigger) error {
	b.wg.Done()
	b.wg.Wait()
	return b.inner.Send(tr)
}

// Nothing in the code claims an outbox row, so two relays can send the same
// reminder. The idempotency key is what keeps it to one acceptance. (In
// production a row claim avoids the wasted call; the key is the backstop.)
func TestTwoRelaysSendingTheSameRowAreDeduplicatedByKey(t *testing.T) {
	f := newFixture(config.Default())
	b := &barrierNotifier{inner: f.n}
	b.wg.Add(2)
	jitter := func() float64 { return 0.5 }
	a := New(config.Default(), f.clk, f.st, b, f.orders, jitter)
	c := New(config.Default(), f.clk, f.st, b, f.orders, jitter)

	var done sync.WaitGroup
	for _, r := range []*Relay{a, c} {
		done.Add(1)
		go func() {
			defer done.Done()
			if err := r.Drain(); err != nil {
				t.Error(err)
			}
		}()
	}
	done.Wait()

	f.wantStatus(t, store.Sent, "")
	if sent, dups := len(f.n.Sent()), f.n.Duplicates(); sent != 1 || dups != 1 {
		t.Fatalf("notifier accepted %d and dropped %d duplicates, want 1 and 1", sent, dups)
	}
}

// The retry scheduler's boundary: a retry landing exactly on the deadline is
// still allowed (a send at exactly the bound is allowed too); one tick later
// is not.
func TestRetryAtTheDeadlineIsAllowedAndOneTickPastIsNot(t *testing.T) {
	cfg := config.Default()
	cfg.BackoffBase, cfg.BackoffMax = 8*time.Second, 8*time.Second // at jitter 0.5 the delay is exactly 6s
	deadline := t0.Add(cfg.MaxLateness)

	tests := []struct {
		name       string
		failAt     time.Time
		wantStatus store.TriggerStatus
		wantReason string
	}{
		{"exactly at the deadline", deadline.Add(-6 * time.Second), store.Pending, ""},
		{"one tick past it", deadline.Add(-6*time.Second + time.Nanosecond), store.Suppressed, "retry_deadline"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFixture(cfg)
			f.orders.SetError(errors.New("timeout"))
			f.clk.Set(tt.failAt)

			f.drain(t)

			f.wantStatus(t, tt.wantStatus, tt.wantReason)
			if tr, _ := f.st.Trigger(key); tt.wantStatus == store.Pending && !tr.NextAttempt.Equal(deadline) {
				t.Fatalf("next attempt at +%v, want the deadline +%v", tr.NextAttempt.Sub(t0), deadline.Sub(t0))
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
