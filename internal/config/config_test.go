package config

import (
	"testing"
	"time"
)

func TestValidate(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Config)
		ok     bool
	}{
		{"defaults", func(*Config) {}, true},
		{"zero offset reminds at abandonment", func(c *Config) { c.Offsets = []time.Duration{0, time.Hour} }, true},
		{"zero window", func(c *Config) { c.Window = 0 }, false},
		{"no offsets", func(c *Config) { c.Offsets = nil }, false},
		{"negative offset", func(c *Config) { c.Offsets = []time.Duration{-time.Minute} }, false},
		{"unsorted offsets", func(c *Config) { c.Offsets = []time.Duration{time.Hour, time.Minute} }, false},
		{"duplicate offsets", func(c *Config) { c.Offsets = []time.Duration{time.Hour, time.Hour} }, false},
		{"zero attempts", func(c *Config) { c.MaxAttempts = 0 }, false},
		{"backoff max below base", func(c *Config) { c.BackoffMax = c.BackoffBase / 2 }, false},
		{"holdout above 100", func(c *Config) { c.HoldoutPercent = 101 }, false},
		{"zero lateness bound", func(c *Config) { c.MaxLateness = 0 }, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := Default()
			tt.mutate(&c)
			if err := c.Validate(); (err == nil) != tt.ok {
				t.Fatalf("Validate() = %v, want ok=%v", err, tt.ok)
			}
		})
	}
}
