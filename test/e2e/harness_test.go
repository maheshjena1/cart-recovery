// Package e2e runs the whole pipeline in process on a fake clock. Each test
// is a timeline: events at given times, the clock advanced, and the exact
// outcomes expected at each step. Run with -v (make demo) to print the
// timelines.
package e2e

import (
	"fmt"
	"slices"
	"testing"
	"time"

	"cartrecovery/internal/clock"
	"cartrecovery/internal/config"
	"cartrecovery/internal/events"
	"cartrecovery/internal/notifier"
	"cartrecovery/internal/pipeline"
	"cartrecovery/internal/store"
)

// start is 00:00 on every timeline.
var start = time.Date(2026, 1, 5, 0, 0, 0, 0, time.UTC)

type scenario struct {
	t      *testing.T
	clk    *clock.Fake
	st     *store.Store
	n      *notifier.Stub
	orders *notifier.Orders
	p      *pipeline.Pipeline

	users   map[string]string // cart id -> user id; default "user-<cart>"
	events  int               // event ids issued
	checked int               // journal entries consumed by expect
	printed int               // journal entries printed
}

func newScenario(t *testing.T, cfg config.Config) *scenario {
	t.Helper()
	s := &scenario{
		t:      t,
		clk:    clock.NewFake(start),
		st:     store.New(),
		n:      notifier.NewStub(),
		orders: notifier.NewOrders(),
		users:  make(map[string]string),
	}
	p, err := pipeline.New(cfg, pipeline.Deps{
		Clock:    s.clk,
		Store:    s.st,
		Notifier: s.n,
		Orders:   s.orders,
		Jitter:   func() float64 { return 0.5 }, // fixed, so retry times are reproducible
	})
	if err != nil {
		t.Fatal(err)
	}
	s.p = p
	return s
}

// at advances the clock to hhmm and ingests a new event for the cart.
func (s *scenario) at(hhmm string, typ events.Type, cart string) *scenario {
	s.t.Helper()
	s.advanceTo(hhmm)
	s.events++
	user, ok := s.users[cart]
	if !ok {
		user = "user-" + cart
	}
	return s.ingest(events.Event{
		ID:         fmt.Sprintf("e%d", s.events),
		CartID:     cart,
		UserID:     user,
		Type:       typ,
		OccurredAt: s.clk.Now(),
	})
}

// ingest delivers ev as is, at the current time. Use it to redeliver an
// event with the same id.
func (s *scenario) ingest(ev events.Event) *scenario {
	s.t.Helper()
	s.say("%s(%s) id=%s", ev.Type, ev.CartID, ev.ID)
	if err := s.p.Ingest(ev); err != nil {
		s.t.Fatal(err)
	}
	s.printJournal()
	return s
}

// advanceTo moves the fake clock to hhmm, firing every timer due on the way.
func (s *scenario) advanceTo(hhmm string) *scenario {
	s.t.Helper()
	var h, m int
	if _, err := fmt.Sscanf(hhmm, "%d:%d", &h, &m); err != nil {
		s.t.Fatalf("bad time %q: %v", hhmm, err)
	}
	s.clk.Set(start.Add(time.Duration(h)*time.Hour + time.Duration(m)*time.Minute))
	s.printJournal()
	return s
}

// expect asserts the outcomes journalled since the previous expect, exactly
// and in order. With no arguments it asserts that nothing happened.
func (s *scenario) expect(want ...string) *scenario {
	s.t.Helper()
	journal := s.st.Journal()
	var got []string
	for _, e := range journal[s.checked:] {
		got = append(got, e.String())
	}
	s.checked = len(journal)
	if !slices.Equal(got, want) {
		s.t.Fatalf("at %s:\n  got  %q\n  want %q", s.now(), got, want)
	}
	return s
}

// expectSent asserts the idempotency keys of everything the notification
// service accepted, in order, and that no trigger reached it twice.
func (s *scenario) expectSent(wantKeys ...string) {
	s.t.Helper()
	var got []string
	for _, tr := range s.n.Sent() {
		got = append(got, tr.Key)
	}
	if !slices.Equal(got, wantKeys) {
		s.t.Fatalf("notifier accepted %q, want %q", got, wantKeys)
	}
	if dups := s.n.Duplicates(); dups != 0 {
		s.t.Fatalf("notifier received %d duplicate deliveries", dups)
	}
}

// restart crashes the pipeline and recovers it, replaying the event log from
// the given offset.
func (s *scenario) restart(fromOffset int) *scenario {
	s.t.Helper()
	s.say("-- crash; restart, replaying the log from offset %d --", fromOffset)
	if err := s.p.Restart(fromOffset); err != nil {
		s.t.Fatal(err)
	}
	s.printJournal()
	return s
}

// replay re-drives a dead-lettered trigger and returns the replay error.
func (s *scenario) replay(key string) error {
	s.say("-- operator replays %s --", key)
	err := s.p.Replay(key)
	if err != nil {
		s.say("   rejected: %v", err)
	}
	s.printJournal()
	return err
}

// say prints a timeline line under -v.
func (s *scenario) say(format string, args ...any) {
	if testing.Verbose() {
		fmt.Printf("    %-12s  %s\n", s.now(), fmt.Sprintf(format, args...))
	}
}

func (s *scenario) printJournal() {
	journal := s.st.Journal()
	if testing.Verbose() {
		for _, e := range journal[s.printed:] {
			fmt.Printf("    %-12s  => %s\n", elapsed(e.At), e)
		}
	}
	s.printed = len(journal)
}

func (s *scenario) now() string { return elapsed(s.clk.Now()) }

// elapsed formats t as time since 00:00: "25:00", or "01:00:02.250" when it
// is not on a whole minute.
func elapsed(t time.Time) string {
	d := t.Sub(start)
	h, m := int(d/time.Hour), int(d%time.Hour/time.Minute)
	if rest := d % time.Minute; rest != 0 {
		return fmt.Sprintf("%02d:%02d:%06.3f", h, m, rest.Seconds())
	}
	return fmt.Sprintf("%02d:%02d", h, m)
}
