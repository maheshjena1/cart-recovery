// Package clock abstracts time so the pipeline runs on the wall clock in
// production and on a deterministic fake in tests.
package clock

import "time"

// Clock is the only source of time in the pipeline.
type Clock interface {
	Now() time.Time
	// AfterFunc calls f once, d from now. A non-positive d means "as soon as
	// possible".
	AfterFunc(d time.Duration, f func()) Timer
}

// Timer is a pending AfterFunc call.
type Timer interface {
	// Stop cancels the call. It reports whether the call was still pending.
	Stop() bool
}

// Real is the wall clock.
type Real struct{}

func (Real) Now() time.Time { return time.Now() }

func (Real) AfterFunc(d time.Duration, f func()) Timer { return time.AfterFunc(d, f) }
