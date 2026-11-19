package e2e

import (
	"fmt"
	"testing"

	"cartrecovery/internal/config"
	"cartrecovery/internal/delivery"
	"cartrecovery/internal/events"
	"cartrecovery/internal/notifier"
)

// Defaults throughout: 30m inactivity window; reminders 30m, 1h and 24h after
// the moment of abandonment.

func TestHappyPathThreeReminders(t *testing.T) {
	s := newScenario(t, config.Default())
	s.at("00:00", events.ItemAdded, "A").
		advanceTo("00:29").expect().
		advanceTo("00:30").expect("Abandoned(A, gen=1)").
		advanceTo("00:59").expect().
		advanceTo("01:00").expect("Trigger(A, offset=30m)").
		advanceTo("01:30").expect("Trigger(A, offset=1h)").
		advanceTo("24:29").expect().
		advanceTo("24:30").expect("Trigger(A, offset=24h)").
		advanceTo("72:00").expect()
	s.expectSent("A:1:0", "A:1:1", "A:1:2")
}

func TestEditResetsTheClock(t *testing.T) {
	s := newScenario(t, config.Default())
	s.at("00:00", events.ItemAdded, "A").
		at("00:10", events.ItemUpdated, "A").
		at("00:25", events.ItemRemoved, "A"). // only the last edit counts
		advanceTo("00:54").expect().
		advanceTo("00:55").expect("Abandoned(A, gen=3)").
		advanceTo("01:25").expect("Trigger(A, offset=30m)").
		advanceTo("72:00").expect("Trigger(A, offset=1h)", "Trigger(A, offset=24h)")
	s.expectSent("A:3:0", "A:3:1", "A:3:2")
}

func TestPurchaseCancelsRemainingReminders(t *testing.T) {
	s := newScenario(t, config.Default())
	s.at("00:00", events.ItemAdded, "A").
		at("00:20", events.ItemUpdated, "A").
		advanceTo("00:50").expect("Abandoned(A, gen=2)").
		advanceTo("01:20").expect("Trigger(A, offset=30m)").
		at("01:25", events.OrderPlaced, "A").
		advanceTo("72:00").expect()
	s.expectSent("A:2:0")
}

// The shopper pays one minute before a reminder is due. The order service
// knows; the OrderPlaced event has not reached the pipeline yet, so the
// cart still looks abandoned when the timer fires. The pre-send check asks
// the order service and stops the reminder.
func TestOrderPlacedBetweenFireAndSendIsCaught(t *testing.T) {
	s := newScenario(t, config.Default())
	s.at("00:00", events.ItemAdded, "A").
		advanceTo("00:30").expect("Abandoned(A, gen=1)").
		advanceTo("00:59")
	s.say("-- order service records an order for A; its event is still in flight --")
	s.orders.Place("A")

	s.advanceTo("01:00").expect("Cancelled(A, offset=30m, order_placed)").
		at("01:01", events.OrderPlaced, "A"). // the event arrives; generation check takes over
		advanceTo("72:00").expect()
	s.expectSent()
}

func TestDuplicateEventDeliveryHasNoEffect(t *testing.T) {
	s := newScenario(t, config.Default())
	s.at("00:00", events.ItemAdded, "A")
	first := events.Event{ID: "e1", CartID: "A", UserID: "user-A", Type: events.ItemAdded}

	// The same event is delivered again at 00:20. Applied twice, it would
	// reset the clock and move abandonment to 00:50.
	s.advanceTo("00:20").ingest(first).
		advanceTo("00:30").expect("Abandoned(A, gen=1)").
		advanceTo("72:00").expect("Trigger(A, offset=30m)", "Trigger(A, offset=1h)", "Trigger(A, offset=24h)")
	s.expectSent("A:1:0", "A:1:1", "A:1:2")
}

// The process dies after the first reminder. Everything in memory is lost;
// the store and the log survive. The new process reprocesses the log from
// the beginning and rebuilds its timers from the store.
func TestRestartAndLogReplaySendNothingTwice(t *testing.T) {
	s := newScenario(t, config.Default())
	s.at("00:00", events.ItemAdded, "A").
		at("00:20", events.ItemUpdated, "A").
		advanceTo("01:20").expect("Abandoned(A, gen=2)", "Trigger(A, offset=30m)").
		restart(0).expect(). // replayed events and the sent reminder are recognised
		advanceTo("01:50").expect("Trigger(A, offset=1h)").
		restart(1).
		advanceTo("72:00").expect("Trigger(A, offset=24h)")
	s.expectSent("A:2:0", "A:2:1", "A:2:2")
}

func TestSendFailuresRetryDeadLetterAndReplay(t *testing.T) {
	s := newScenario(t, config.Default())
	s.at("00:00", events.ItemAdded, "A").
		advanceTo("00:30").expect("Abandoned(A, gen=1)")

	// Transient: two timeouts, then success, with backoff in between.
	s.n.FailNext(2, notifier.ErrTransient)
	s.advanceTo("01:01").expect(
		"Retry(A, offset=30m, attempt=1)",
		"Retry(A, offset=30m, attempt=2)",
		"Trigger(A, offset=30m)",
	)

	// Permanent: no retry, straight to the dead-letter queue with the reason.
	s.n.FailNext(1, fmt.Errorf("template missing: %w", notifier.ErrPermanent))
	s.advanceTo("01:30").expect("DeadLettered(A, offset=1h, template missing: permanent failure)")
	if dlq := s.st.DeadLetters(); len(dlq) != 1 || dlq[0].Key != "A:1:1" {
		t.Fatalf("dead-letter queue = %+v, want A:1:1", dlq)
	}

	// The operator fixes the template and replays. A second replay of the
	// same trigger is refused.
	s.advanceTo("01:35")
	if err := s.replay("A:1:1"); err != nil {
		t.Fatal(err)
	}
	s.expect("Replayed(A, offset=1h)", "Trigger(A, offset=1h)")
	if err := s.replay("A:1:1"); err == nil {
		t.Fatal("second replay of a sent trigger was accepted")
	}
	s.expect()

	s.advanceTo("72:00").expect("Trigger(A, offset=24h)")
	s.expectSent("A:1:0", "A:1:1", "A:1:2")
}

// Two shoppers abandon identical carts. One is in the holdout: the pipeline
// does all the same work for both and records what it would have sent, but
// only the treated shopper is notified.
func TestHoldoutIsRecordedButNotSent(t *testing.T) {
	cfg := config.Default()
	cfg.HoldoutPercent = 50
	held, treated := pickShoppers(cfg)

	s := newScenario(t, cfg)
	s.users["H"], s.users["T"] = held, treated
	s.at("00:00", events.ItemAdded, "H").
		at("00:00", events.ItemAdded, "T").
		advanceTo("00:30").expect("Abandoned(H, gen=1)", "Abandoned(T, gen=1)").
		advanceTo("72:00").expect(
		"Suppressed(H, offset=30m, holdout)", "Trigger(T, offset=30m)",
		"Suppressed(H, offset=1h, holdout)", "Trigger(T, offset=1h)",
		"Suppressed(H, offset=24h, holdout)", "Trigger(T, offset=24h)",
	)
	s.expectSent("T:1:0", "T:1:1", "T:1:2")
}

// pickShoppers returns one user id inside the holdout and one outside it.
func pickShoppers(cfg config.Config) (held, treated string) {
	for i := 0; held == "" || treated == ""; i++ {
		id := fmt.Sprintf("user-%d", i)
		if delivery.InHoldout(id, cfg.HoldoutSalt, cfg.HoldoutPercent) {
			held = id
		} else {
			treated = id
		}
	}
	return held, treated
}
