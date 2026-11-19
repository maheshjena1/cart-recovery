package scheduler

import (
	"errors"
	"slices"
	"testing"
	"time"

	"cartrecovery/internal/clock"
	"cartrecovery/internal/store"
)

var t0 = time.Date(2026, 1, 5, 0, 0, 0, 0, time.UTC)

type firing struct {
	id string
	at time.Duration // since t0
}

func addTimer(st *store.Store, id string, due time.Duration) {
	_ = st.Update(func(tx *store.Tx) error {
		tx.AddTimer(store.Timer{ID: id, Due: t0.Add(due)})
		return nil
	})
}

func TestFiresAtDueTimeInDeterministicOrder(t *testing.T) {
	clk, st := clock.NewFake(t0), store.New()
	var got []firing
	s := New(clk, st, func(timer store.Timer) error {
		got = append(got, firing{timer.ID, clk.Now().Sub(t0)})
		return nil
	})

	addTimer(st, "late", 3*time.Hour)
	addTimer(st, "tie-b", time.Hour)
	addTimer(st, "tie-a", time.Hour)
	s.Arm()
	addTimer(st, "early", time.Minute) // earlier than the pending wake-up
	s.Arm()

	clk.Advance(24 * time.Hour)

	want := []firing{{"early", time.Minute}, {"tie-a", time.Hour}, {"tie-b", time.Hour}, {"late", 3 * time.Hour}}
	if !slices.Equal(got, want) {
		t.Fatalf("fired %v, want %v", got, want)
	}
	if _, ok := st.NextTimer(); ok {
		t.Fatal("fired timers were not deleted")
	}
}

func TestTimerAddedByHandlerFires(t *testing.T) {
	clk, st := clock.NewFake(t0), store.New()
	var got []firing
	s := New(clk, st, func(timer store.Timer) error {
		got = append(got, firing{timer.ID, clk.Now().Sub(t0)})
		if timer.ID == "first" {
			addTimer(st, "follow-up", 2*time.Minute)
		}
		return nil
	})
	addTimer(st, "first", time.Minute)
	s.Arm()

	clk.Advance(time.Hour)

	want := []firing{{"first", time.Minute}, {"follow-up", 2 * time.Minute}}
	if !slices.Equal(got, want) {
		t.Fatalf("fired %v, want %v", got, want)
	}
}

func TestFailedTimerIsRedelivered(t *testing.T) {
	clk, st := clock.NewFake(t0), store.New()
	var got []firing
	failures := 2
	s := New(clk, st, func(timer store.Timer) error {
		got = append(got, firing{timer.ID, clk.Now().Sub(t0)})
		if timer.ID == "flaky" && failures > 0 {
			failures--
			return errors.New("store unavailable")
		}
		return nil
	})
	addTimer(st, "flaky", time.Minute)
	addTimer(st, "next", time.Minute+time.Millisecond)
	s.Arm()

	clk.Advance(time.Hour)

	want := []firing{
		{"flaky", time.Minute},
		{"flaky", time.Minute + retryDelay},
		{"flaky", time.Minute + 2*retryDelay},
		{"next", time.Minute + 2*retryDelay}, // held behind the failing timer
	}
	if !slices.Equal(got, want) {
		t.Fatalf("fired %v, want %v", got, want)
	}
}

func TestRestartedSchedulerResumesFromStore(t *testing.T) {
	clk, st := clock.NewFake(t0), store.New()
	var got []string
	record := func(timer store.Timer) error {
		got = append(got, timer.ID)
		return nil
	}
	addTimer(st, "before-crash", time.Minute)
	addTimer(st, "after-crash", time.Hour)
	addTimer(st, "due-while-down", 10*time.Minute)

	old := New(clk, st, record)
	old.Arm()
	clk.Advance(5 * time.Minute)
	old.Stop() // crash

	clk.Advance(20 * time.Minute) // nothing is running
	if want := []string{"before-crash"}; !slices.Equal(got, want) {
		t.Fatalf("while down, fired %v, want %v", got, want)
	}

	New(clk, st, record).Arm() // restart: no state handed over
	clk.Advance(24 * time.Hour)

	want := []string{"before-crash", "due-while-down", "after-crash"}
	if !slices.Equal(got, want) {
		t.Fatalf("fired %v, want %v", got, want)
	}
}
