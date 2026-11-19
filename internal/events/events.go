// Package events defines the cart events the pipeline consumes.
package events

import (
	"errors"
	"fmt"
	"time"
)

type Type string

const (
	ItemAdded       Type = "ItemAdded"
	ItemUpdated     Type = "ItemUpdated"
	ItemRemoved     Type = "ItemRemoved"
	CheckoutStarted Type = "CheckoutStarted"
	CartResumed     Type = "CartResumed" // shopper returned to, or viewed, the cart
	CartCleared     Type = "CartCleared"
	OrderPlaced     Type = "OrderPlaced"
)

// Event is one cart event as published by the storefront.
//
// OccurredAt and Seq are producer-side metadata, kept for audit. The pipeline
// does not order by them: per-cart order is the order of the partitioned log,
// and the inactivity clock runs on server ingest time (DESIGN.md, section 2).
type Event struct {
	ID         string // unique per logical event; the dedupe key
	CartID     string
	UserID     string // set for signed-in shoppers
	SessionID  string // set for guests
	Type       Type
	OccurredAt time.Time
	Seq        int64
}

// Validate rejects events the pipeline cannot route or attribute. It runs at
// the ingest edge, before the event reaches the log.
func (e Event) Validate() error {
	switch {
	case e.ID == "":
		return errors.New("event: missing id")
	case e.CartID == "":
		return errors.New("event: missing cartId")
	case e.UserID == "" && e.SessionID == "":
		return errors.New("event: needs userId or sessionId")
	}
	switch e.Type {
	case ItemAdded, ItemUpdated, ItemRemoved, CheckoutStarted, CartResumed, CartCleared, OrderPlaced:
		return nil
	}
	return fmt.Errorf("event: unknown type %q", e.Type)
}
