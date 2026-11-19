package clock

import (
	"slices"
	"testing"
	"time"
)

var t0 = time.Date(2026, 1, 5, 0, 0, 0, 0, time.UTC)

func TestFakeFiresInDueOrderThenRegistrationOrder(t *testing.T) {
	c := NewFake(t0)
	var got []string
	add := func(name string, d time.Duration) {
		c.AfterFunc(d, func() { got = append(got, name) })
	}
	add("late", 3*time.Minute)
	add("tie-first", 2*time.Minute)
	add("tie-second", 2*time.Minute)
	add("early", time.Minute)
	add("beyond", time.Hour)

	c.Advance(10 * time.Minute)

	want := []string{"early", "tie-first", "tie-second", "late"}
	if !slices.Equal(got, want) {
		t.Fatalf("fired %v, want %v", got, want)
	}
	if !c.Now().Equal(t0.Add(10 * time.Minute)) {
		t.Fatalf("Now = %v, want start+10m", c.Now())
	}
}

func TestFakeNowIsDueTimeInsideCallback(t *testing.T) {
	c := NewFake(t0)
	var at time.Time
	c.AfterFunc(7*time.Minute, func() { at = c.Now() })
	c.Advance(time.Hour)
	if !at.Equal(t0.Add(7 * time.Minute)) {
		t.Fatalf("callback saw %v, want start+7m", at)
	}
}

func TestFakeTimerRegisteredByCallbackFiresInSameAdvance(t *testing.T) {
	c := NewFake(t0)
	var got []time.Duration
	c.AfterFunc(time.Minute, func() {
		got = append(got, c.Now().Sub(t0))
		c.AfterFunc(time.Minute, func() { got = append(got, c.Now().Sub(t0)) })
	})
	c.Advance(5 * time.Minute)
	want := []time.Duration{time.Minute, 2 * time.Minute}
	if !slices.Equal(got, want) {
		t.Fatalf("fired at %v, want %v", got, want)
	}
}

func TestFakeStop(t *testing.T) {
	c := NewFake(t0)
	fired := false
	timer := c.AfterFunc(time.Minute, func() { fired = true })
	if !timer.Stop() {
		t.Fatal("Stop on a pending timer returned false")
	}
	c.Advance(time.Hour)
	if fired {
		t.Fatal("stopped timer fired")
	}
	if timer.Stop() {
		t.Fatal("second Stop returned true")
	}
}

func TestFakeSetBackwardsPanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("Set to the past did not panic")
		}
	}()
	NewFake(t0).Set(t0.Add(-time.Second))
}
