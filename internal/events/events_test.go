package events

import "testing"

func TestValidate(t *testing.T) {
	valid := Event{ID: "e1", CartID: "c1", SessionID: "s1", Type: ItemAdded}
	tests := []struct {
		name   string
		mutate func(*Event)
		ok     bool
	}{
		{"guest event", func(*Event) {}, true},
		{"signed-in event", func(e *Event) { e.SessionID, e.UserID = "", "u1" }, true},
		{"missing id", func(e *Event) { e.ID = "" }, false},
		{"missing cart", func(e *Event) { e.CartID = "" }, false},
		{"no identity", func(e *Event) { e.SessionID = "" }, false},
		{"unknown type", func(e *Event) { e.Type = "CartExploded" }, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := valid
			tt.mutate(&e)
			if err := e.Validate(); (err == nil) != tt.ok {
				t.Fatalf("Validate() = %v, want ok=%v", err, tt.ok)
			}
		})
	}
}
