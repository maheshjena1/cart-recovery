// Package notifier holds the contract with the downstream notification
// service and in-memory stand-ins for it and for the order service. Nothing
// here sends anything: the stub records the triggers it is handed.
package notifier

import (
	"errors"
	"slices"
	"sync"
	"time"
)

// Trigger is what the pipeline hands to the notification service. It carries
// ids only. The service resolves the shopper's contact details, consent and
// cart contents itself at render time, so no personal data crosses this
// boundary or rests in the pipeline's store.
type Trigger struct {
	Key       string // idempotency key; the service must drop repeats
	CartID    string
	ShopperID string
	Offset    time.Duration
	DueAt     time.Time
}

var (
	// ErrTransient is a failure worth retrying, such as a timeout.
	ErrTransient = errors.New("transient failure")
	// ErrPermanent marks a failure that retrying cannot fix. Wrap it.
	ErrPermanent = errors.New("permanent failure")
)

// Stub is a recording notification service with failure injection. Like the
// real service must, it drops triggers whose Key it has already accepted.
type Stub struct {
	mu       sync.Mutex
	sent     []Trigger
	accepted map[string]bool
	dups     int
	attempts int
	failNext int
	failErr  error
	loseAcks int
}

func NewStub() *Stub { return &Stub{accepted: make(map[string]bool)} }

func (s *Stub) Send(t Trigger) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.attempts++
	if s.failNext > 0 {
		s.failNext--
		return s.failErr
	}
	if s.accepted[t.Key] {
		s.dups++
	} else {
		s.accepted[t.Key] = true
		s.sent = append(s.sent, t)
	}
	if s.loseAcks > 0 {
		s.loseAcks--
		return ErrTransient // accepted, but the caller never learns it
	}
	return nil
}

// FailNext makes the next n calls to Send fail with err before the trigger
// is accepted.
func (s *Stub) FailNext(n int, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failNext, s.failErr = n, err
}

// LoseAckNext makes the next n calls to Send accept the trigger and then
// report a transient error, as when the response is lost on the way back.
func (s *Stub) LoseAckNext(n int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.loseAcks = n
}

// Sent returns the distinct triggers accepted, in order.
func (s *Stub) Sent() []Trigger {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.sent)
}

// Duplicates is how many repeat deliveries were dropped by key.
func (s *Stub) Duplicates() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.dups
}

// Attempts is the total number of Send calls.
func (s *Stub) Attempts() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.attempts
}

// Orders is a stand-in for the order service, the authority on whether a
// cart has been purchased.
type Orders struct {
	mu     sync.Mutex
	placed map[string]bool
	err    error
}

func NewOrders() *Orders { return &Orders{placed: make(map[string]bool)} }

// Place records an order for the cart.
func (o *Orders) Place(cartID string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.placed[cartID] = true
}

// SetError makes Placed fail with err until cleared with nil.
func (o *Orders) SetError(err error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.err = err
}

func (o *Orders) Placed(cartID string) (bool, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.err != nil {
		return false, o.err
	}
	return o.placed[cartID], nil
}
