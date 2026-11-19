package delivery

import (
	"hash/fnv"
	"time"
)

// Backoff returns the delay before retry number attempt (1-based). The delay
// doubles from base up to max, then jitter in [0, 1) picks a point in the
// upper half of it: at least half the nominal delay, so retries always back
// off, and spread out, so a burst of failures does not retry in step.
func Backoff(attempt int, base, max time.Duration, jitter float64) time.Duration {
	d := base
	for i := 1; i < attempt && d < max; i++ {
		d *= 2
	}
	if d > max {
		d = max
	}
	half := d / 2
	return half + time.Duration(jitter*float64(d-half))
}

// InHoldout reports whether a shopper is in the control group. Assignment is
// a pure function of the shopper id and the salt, so it is stable across
// carts, restarts and replays, and needs no storage.
func InHoldout(shopperID, salt string, percent int) bool {
	h := fnv.New32a()
	h.Write([]byte(salt))
	h.Write([]byte{0})
	h.Write([]byte(shopperID))
	return int(h.Sum32()%100) < percent
}
