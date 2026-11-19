// Package config holds the tunable behaviour of the recovery pipeline.
package config

import (
	"errors"
	"fmt"
	"time"
)

type Config struct {
	// Window is how long a cart must be inactive to count as abandoned.
	Window time.Duration
	// Offsets are the reminder times, measured from the moment of abandonment
	// (last activity + Window). Strictly increasing. Zero is allowed and means
	// "remind at the moment of abandonment".
	Offsets []time.Duration
	// MaxLateness is the staleness bound: a reminder that cannot be sent
	// within this long of its due time is suppressed, not sent late.
	MaxLateness time.Duration

	// MaxAttempts caps delivery attempts per reminder before it is
	// dead-lettered.
	MaxAttempts int
	// BackoffBase and BackoffMax bound the exponential retry delay.
	BackoffBase time.Duration
	BackoffMax  time.Duration

	// HoldoutPercent of shoppers are a control group: their reminders are
	// computed and recorded but never sent.
	HoldoutPercent int
	// HoldoutSalt decorrelates this holdout from other experiments that hash
	// the same shopper id.
	HoldoutSalt string
}

// Default returns the defaults from the problem statement. The holdout is off
// by default; it is an experiment setting, enabled per rollout stage.
func Default() Config {
	return Config{
		Window:      30 * time.Minute,
		Offsets:     []time.Duration{30 * time.Minute, time.Hour, 24 * time.Hour},
		MaxLateness: 30 * time.Minute,
		MaxAttempts: 5,
		BackoffBase: time.Second,
		BackoffMax:  5 * time.Minute,
		HoldoutSalt: "cart-recovery-v1",
	}
}

func (c Config) Validate() error {
	switch {
	case c.Window <= 0:
		return errors.New("config: window must be positive")
	case len(c.Offsets) == 0:
		return errors.New("config: at least one reminder offset is required")
	case c.MaxLateness <= 0:
		return errors.New("config: maxLateness must be positive")
	case c.MaxAttempts < 1:
		return errors.New("config: maxAttempts must be at least 1")
	case c.BackoffBase <= 0 || c.BackoffMax < c.BackoffBase:
		return errors.New("config: need 0 < backoffBase <= backoffMax")
	case c.HoldoutPercent < 0 || c.HoldoutPercent > 100:
		return errors.New("config: holdoutPercent must be between 0 and 100")
	}
	prev := time.Duration(-1)
	for i, off := range c.Offsets {
		if off <= prev {
			return fmt.Errorf("config: offsets must be non-negative and strictly increasing (index %d)", i)
		}
		prev = off
	}
	return nil
}
