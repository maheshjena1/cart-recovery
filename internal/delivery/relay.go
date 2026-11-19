// Package delivery drains the trigger outbox to the notification service:
// last-moment cancellation checks, retries with backoff, a dead-letter queue
// and replay.
//
// Delivery is at least once. A crash, or a lost response, between the
// service accepting a trigger and the outbox row being marked SENT causes a
// second delivery with the same idempotency key, which the service drops.
package delivery

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"cartrecovery/internal/clock"
	"cartrecovery/internal/config"
	"cartrecovery/internal/notifier"
	"cartrecovery/internal/store"
)

// Notifier is the downstream notification service.
type Notifier interface {
	Send(notifier.Trigger) error
}

// Orders is the order service, the authority on purchases.
type Orders interface {
	Placed(cartID string) (bool, error)
}

type Relay struct {
	cfg    config.Config
	clk    clock.Clock
	st     *store.Store
	n      Notifier
	orders Orders
	jitter func() float64

	mu sync.Mutex // one delivery pass at a time
}

// New returns a relay. jitter returns a number in [0, 1) used to spread
// retries; production passes rand.Float64.
func New(cfg config.Config, clk clock.Clock, st *store.Store, n Notifier, orders Orders, jitter func() float64) *Relay {
	return &Relay{cfg: cfg, clk: clk, st: st, n: n, orders: orders, jitter: jitter}
}

// Drain attempts every outbox row that is ready now.
func (r *Relay) Drain() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.drain()
}

func (r *Relay) drain() error {
	for _, tr := range r.st.PendingTriggers(r.clk.Now()) {
		if err := r.deliver(tr); err != nil {
			return err
		}
	}
	return nil
}

// deliver runs the pre-send checks, cheapest first, then sends. The holdout
// check is deliberately last: a holdout shopper's reminder is recorded as
// suppressed only if a treated shopper's would actually have been sent.
//
// The clock is read per trigger, and again after the order-service call:
// that call can be slow, and a pass over many triggers can take a while, so
// a time read at the start of the pass says nothing about when the send
// happens.
func (r *Relay) deliver(tr store.Trigger) error {
	now := r.clk.Now()
	cart, _ := r.st.Cart(tr.CartID)
	if cart.Generation != tr.Generation || cart.Status != store.Abandoned {
		return r.finish(tr, now, store.Cancelled, "cart_changed")
	}
	if now.Sub(tr.DueAt) > r.cfg.MaxLateness {
		return r.finish(tr, now, store.Suppressed, "stale")
	}
	// The cart's own state can trail the order service by the lag of the
	// event log. Ask the authority. If it cannot answer, do not send blind.
	placed, err := r.orders.Placed(tr.CartID)
	now = r.clk.Now()
	if err != nil {
		// Already past the bound: suppress. Dead-lettering would queue a
		// reminder that a replay could only suppress too.
		if now.Sub(tr.DueAt) > r.cfg.MaxLateness {
			return r.finish(tr, now, store.Suppressed, "stale")
		}
		return r.retry(tr, now, fmt.Errorf("order check: %w", err))
	}
	if placed {
		return r.finish(tr, now, store.Cancelled, "order_placed")
	}
	// The engine keeps committing events while the call above is in flight.
	// Look at the cart once more, as late as possible. What remains after
	// this is the handoff window described in DESIGN section 6.
	cart, _ = r.st.Cart(tr.CartID)
	if cart.Generation != tr.Generation || cart.Status != store.Abandoned {
		return r.finish(tr, now, store.Cancelled, "cart_changed")
	}
	if now.Sub(tr.DueAt) > r.cfg.MaxLateness {
		return r.finish(tr, now, store.Suppressed, "stale")
	}
	if InHoldout(tr.ShopperID, r.cfg.HoldoutSalt, r.cfg.HoldoutPercent) {
		return r.finish(tr, now, store.Suppressed, "holdout")
	}

	err = r.n.Send(notifier.Trigger{
		Key:       tr.Key,
		CartID:    tr.CartID,
		ShopperID: tr.ShopperID,
		Offset:    tr.Offset,
		DueAt:     tr.DueAt,
	})
	switch {
	case err == nil:
		return r.finish(tr, now, store.Sent, "")
	case errors.Is(err, notifier.ErrPermanent):
		return r.finish(tr, now, store.Dead, err.Error())
	default:
		return r.retry(tr, now, err)
	}
}

// retry schedules another attempt. It dead-letters the trigger when the
// attempt cap is reached, or when the next attempt would land past the
// staleness bound: such an attempt would be suppressed as stale anyway, so
// the reminder is surfaced for an operator now rather than at the bound.
func (r *Relay) retry(tr store.Trigger, now time.Time, cause error) error {
	tr.Attempts++
	if tr.Attempts >= r.cfg.MaxAttempts {
		return r.finish(tr, now, store.Dead, fmt.Sprintf("gave up after %d attempts: %v", tr.Attempts, cause))
	}
	next := now.Add(Backoff(tr.Attempts, r.cfg.BackoffBase, r.cfg.BackoffMax, r.jitter()))
	if next.After(tr.DueAt.Add(r.cfg.MaxLateness)) {
		return r.finish(tr, now, store.Dead, fmt.Sprintf("next retry would pass the staleness bound after %d attempts: %v", tr.Attempts, cause))
	}
	tr.NextAttempt = next
	return r.st.Update(func(tx *store.Tx) error {
		tx.PutTrigger(tr)
		tx.AddTimer(store.Timer{
			ID:     fmt.Sprintf("retry/%s/%d", tr.Key, tr.Attempts),
			Kind:   store.Retry,
			Due:    tr.NextAttempt,
			CartID: tr.CartID,
		})
		tx.Record(entry(tr, now, store.KindRetry, fmt.Sprintf("attempt=%d", tr.Attempts)))
		return nil
	})
}

var kindOf = map[store.TriggerStatus]string{
	store.Sent:       store.KindTrigger,
	store.Cancelled:  store.KindCancelled,
	store.Suppressed: store.KindSuppressed,
	store.Dead:       store.KindDeadLettered,
}

// finish moves the trigger to a final status and journals the outcome.
func (r *Relay) finish(tr store.Trigger, now time.Time, status store.TriggerStatus, reason string) error {
	tr.Status, tr.Reason = status, reason
	return r.st.Update(func(tx *store.Tx) error {
		tx.PutTrigger(tr)
		tx.Record(entry(tr, now, kindOf[status], reason))
		return nil
	})
}

// Replay puts a dead-lettered trigger back in the outbox with a fresh attempt
// budget and tries it. It is safe to run at any time: the pre-send checks run
// again, so a trigger whose cart was since purchased is cancelled and one
// past the staleness bound is suppressed. A trigger that is not dead-lettered
// is rejected, so replaying twice cannot send twice.
func (r *Relay) Replay(key string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	tr, ok := r.st.Trigger(key)
	if !ok || tr.Status != store.Dead {
		return fmt.Errorf("replay %s: not in the dead-letter queue", key)
	}
	now := r.clk.Now()
	tr.Status, tr.Reason, tr.Attempts, tr.NextAttempt = store.Pending, "", 0, time.Time{}
	err := r.st.Update(func(tx *store.Tx) error {
		tx.PutTrigger(tr)
		tx.Record(entry(tr, now, store.KindReplayed, ""))
		return nil
	})
	if err != nil {
		return err
	}
	return r.drain()
}

func entry(tr store.Trigger, now time.Time, kind, detail string) store.Entry {
	return store.Entry{At: now, Kind: kind, CartID: tr.CartID, Generation: tr.Generation, Offset: tr.Offset, Detail: detail}
}
