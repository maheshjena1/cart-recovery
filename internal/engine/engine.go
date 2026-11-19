// Package engine is the cart state machine: it detects abandonment and
// schedules reminders. Each handler is one store transaction, so a state
// change and the timers or triggers it produces commit together.
//
// Both handlers are idempotent. The log and the timer store deliver at least
// once, and running the same event or timer again changes nothing.
package engine

import (
	"fmt"
	"time"

	"cartrecovery/internal/clock"
	"cartrecovery/internal/config"
	"cartrecovery/internal/events"
	"cartrecovery/internal/store"
)

type Engine struct {
	cfg config.Config
	clk clock.Clock
	st  *store.Store
}

func New(cfg config.Config, clk clock.Clock, st *store.Store) *Engine {
	return &Engine{cfg: cfg, clk: clk, st: st}
}

// TriggerKey is the idempotency key of one reminder: a cart, the generation
// it was abandoned under, and which offset. A new cycle after an edit has a
// new generation and therefore new keys.
func TriggerKey(cartID string, generation uint64, offsetIndex int) string {
	return fmt.Sprintf("%s:%d:%d", cartID, generation, offsetIndex)
}

// HandleEvent applies one cart event. ingestedAt is the time the log accepted
// the event; it drives the inactivity clock, so replaying the log reproduces
// the same state.
func (e *Engine) HandleEvent(ev events.Event, ingestedAt time.Time) error {
	return e.st.Update(func(tx *store.Tx) error {
		if tx.EventSeen(ev.ID) {
			return nil // redelivery or replay
		}
		tx.MarkEventSeen(ev.ID)

		old, exists := tx.Cart(ev.CartID)
		if exists && ev.UserID != "" && old.UserID != "" && ev.UserID != old.UserID {
			// Someone else's event on this cart. It must not reset the
			// owner's activity or cancel their reminders, so it does not
			// apply. It stays marked as seen, and the conflict is journalled.
			tx.Record(store.Entry{
				At: e.clk.Now(), Kind: store.KindConflict, CartID: old.ID, Generation: old.Generation,
				Detail: fmt.Sprintf("event %s names %s, cart belongs to %s", ev.ID, ev.UserID, old.UserID),
			})
			return nil
		}
		c, changed := transition(old, exists, ev, ingestedAt)
		if !changed {
			return nil
		}
		bindIdentity(&c, ev)
		// One inactivity timer per cart. If one is already armed it is left
		// alone: when it fires it sees the newer LastActivity and re-arms.
		if c.Status == store.Active && c.CheckDue.IsZero() {
			c.CheckDue = ingestedAt.Add(e.cfg.Window)
			tx.AddTimer(inactivityTimer(c))
		}
		tx.PutCart(c)
		return nil
	})
}

// bindIdentity keeps the cart's identity current as events arrive. A guest
// cart (session id only) is upgraded to the user's id on the first event that
// carries one, so reminders are addressed to, and the holdout is decided on,
// the signed-in shopper. It never goes the other way. HandleEvent has already
// dropped events naming a different user, so the cart's user never changes
// once set.
func bindIdentity(c *store.Cart, ev events.Event) {
	if c.SessionID == "" {
		c.SessionID = ev.SessionID
	}
	if c.UserID == "" {
		c.UserID = ev.UserID // guest signed in
	}
}

// transition returns the cart after ev, and false if ev does not apply.
// Every applied event bumps the generation, which voids all reminders
// scheduled before it.
func transition(c store.Cart, exists bool, ev events.Event, at time.Time) (store.Cart, bool) {
	opensCart := ev.Type == events.ItemAdded || ev.Type == events.ItemUpdated
	switch {
	case !exists:
		if !opensCart {
			return c, false
		}
		c = store.Cart{ID: ev.CartID, UserID: ev.UserID, SessionID: ev.SessionID}
	case c.Status == store.Purchased:
		return c, false // terminal: the next purchase journey uses a new cart id
	case c.Status == store.Cleared && !opensCart:
		return c, false // viewing an empty cart is not activity
	}

	c.Generation++
	switch ev.Type {
	case events.OrderPlaced:
		c.Status = store.Purchased
	case events.CartCleared:
		c.Status = store.Cleared
	default: // item edits, checkout started, cart resumed
		c.Status = store.Active
		c.LastActivity = at
		c.AbandonedAt = time.Time{}
	}
	return c, true
}

// HandleTimer applies one fired timer.
func (e *Engine) HandleTimer(t store.Timer) error {
	return e.st.Update(func(tx *store.Tx) error {
		c, ok := tx.Cart(t.CartID)
		if !ok {
			return nil
		}
		switch t.Kind {
		case store.Inactivity:
			e.checkInactivity(tx, c)
		case store.Reminder:
			enqueueReminder(tx, c, t)
		}
		return nil
	})
}

// checkInactivity decides from the cart's current state alone, never from the
// timer's contents, and timer IDs are deterministic. A duplicate or late
// firing therefore reaches the same decision and re-adds the same timers.
func (e *Engine) checkInactivity(tx *store.Tx, c store.Cart) {
	c.CheckDue = time.Time{}
	deadline := c.LastActivity.Add(e.cfg.Window)
	switch {
	case c.Status != store.Active:
		// Purchased, cleared or already abandoned. Nothing to detect; the
		// next activity event arms a new timer.
	case deadline.After(e.clk.Now()):
		// Edited since this timer was armed: the clock was reset.
		c.CheckDue = deadline
		tx.AddTimer(inactivityTimer(c))
	default:
		// Offsets are anchored to the deadline, not to now, so a late timer
		// does not shift the reminder schedule.
		c.Status = store.Abandoned
		c.AbandonedAt = deadline
		for i, offset := range e.cfg.Offsets {
			tx.AddTimer(store.Timer{
				ID:          "remind/" + TriggerKey(c.ID, c.Generation, i),
				Kind:        store.Reminder,
				Due:         deadline.Add(offset),
				CartID:      c.ID,
				Generation:  c.Generation,
				OffsetIndex: i,
				Offset:      offset,
			})
		}
		tx.Record(store.Entry{At: e.clk.Now(), Kind: store.KindAbandoned, CartID: c.ID, Generation: c.Generation})
	}
	tx.PutCart(c)
}

// enqueueReminder writes the reminder to the outbox, unless the cart moved on
// since the timer was scheduled or the reminder is already there.
func enqueueReminder(tx *store.Tx, c store.Cart, t store.Timer) {
	if c.Generation != t.Generation || c.Status != store.Abandoned {
		return // cancelled: edited, resumed, cleared or purchased
	}
	key := TriggerKey(c.ID, t.Generation, t.OffsetIndex)
	if tx.TriggerExists(key) {
		return // duplicate firing
	}
	tx.PutTrigger(store.Trigger{
		Key:         key,
		CartID:      c.ID,
		ShopperID:   c.ShopperID(),
		Generation:  t.Generation,
		OffsetIndex: t.OffsetIndex,
		Offset:      t.Offset,
		DueAt:       t.Due,
		Status:      store.Pending,
	})
}

func inactivityTimer(c store.Cart) store.Timer {
	return store.Timer{
		ID:     fmt.Sprintf("check/%s/%d", c.ID, c.CheckDue.UnixNano()),
		Kind:   store.Inactivity,
		Due:    c.CheckDue,
		CartID: c.ID,
	}
}
