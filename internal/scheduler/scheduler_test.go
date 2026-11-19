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

// A retry timer fires and is deleted; a replay later re-adds the same ID,
// later still, while another timer is due sooner. Due order must hold. (The
// leftover-heap-entry hazard itself is pinned in the store test; the
// scheduler alone cannot interleave an add between delete and next lookup.)
func TestTimerIDCanBeReusedAfterItFired(t *testing.T) {
	clk, st := clock.NewFake(t0), store.New()
	var got []firing
	s := New(clk, st, func(timer store.Timer) error {
		got = append(got, firing{timer.ID, clk.Now().Sub(t0)})
		if timer.ID == "replay" { // the retry timer is long gone by now
			addTimer(st, "retry/k/1", 3*time.Hour) // reused ID, later due
			addTimer(st, "other", 2*time.Hour)
		}
		return nil
	})
	addTimer(st, "retry/k/1", time.Minute)
	addTimer(st, "replay", 30*time.Minute)
	s.Arm()

	clk.Advance(24 * time.Hour)

	want := []firing{{"retry/k/1", time.Minute}, {"replay", 30 * time.Minute}, {"other", 2 * time.Hour}, {"retry/k/1", 3 * time.Hour}}
	if !slices.Equal(got, want) {
		t.Fatalf("fired %v, want %v", got, want)
	}
}

func TestStopWithAWakeUpPendingFiresNothing(t *testing.T) {
	clk, st := clock.NewFake(t0), store.New()
	fired := false
	s := New(clk, st, func(store.Timer) error { fired = true; return nil })
	addTimer(st, "due-later", time.Hour)
	s.Arm() // a wake-up is now pending on the clock

	s.Stop()
	clk.Advance(24 * time.Hour)

	if fired {
		t.Fatal("timer fired after Stop")
	}
	if _, ok := st.NextTimer(); !ok {
		t.Fatal("Stop removed the timer; it must stay in the store for the next scheduler")
	}
}

// With a real clock the callback runs on its own goroutine, so Stop can be
// called while a timer is mid-fire. Stop must wait for it, and nothing may
// fire after Stop returns.
func TestStopWaitsForARunningCallbackAndNothingFiresAfter(t *testing.T) {
	clk, st := clock.NewFake(t0), store.New()
	entered, release := make(chan struct{}), make(chan struct{})
	var fires int
	s := New(clk, st, func(store.Timer) error {
		fires++
		if fires == 1 {
			close(entered)
			<-release // mid-fire
		}
		return nil
	})
	addTimer(st, "a", time.Minute)
	s.Arm()

	advanced := make(chan struct{})
	go func() { clk.Advance(time.Hour); close(advanced) }()
	<-entered

	stopped := make(chan struct{})
	go func() { s.Stop(); close(stopped) }()
	select {
	case <-stopped:
		t.Fatal("Stop returned while a timer was still firing")
	case <-time.After(50 * time.Millisecond):
	}

	close(release)
	<-advanced
	<-stopped

	before := fires
	addTimer(st, "b", 2*time.Hour)
	s.Arm()
	clk.Advance(24 * time.Hour)
	if fires != before {
		t.Fatalf("%d timers fired after Stop returned", fires-before)
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
