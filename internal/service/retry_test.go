package service

import (
	"testing"
	"time"
)

func TestBackoffPolicy_DefaultSchedule(t *testing.T) {
	p := NewBackoffPolicy(nil)

	if got := p.MaxAttempts(); got != 5 {
		t.Fatalf("MaxAttempts = %d, want 5 (1 immediate + 4 retries)", got)
	}

	cases := []struct {
		attemptsMade int
		wantDelay    time.Duration
		wantRetry    bool
	}{
		{0, 0, true},                    // before first attempt: immediate
		{1, 30 * time.Second, true},     // after attempt 1 failed
		{2, 2 * time.Minute, true},      // after attempt 2 failed
		{3, 10 * time.Minute, true},     // after attempt 3 failed
		{4, 1 * time.Hour, true},        // after attempt 4 failed
		{5, 0, false},                   // after attempt 5 failed: dead-letter
		{6, 0, false},                   // stays exhausted
	}
	for _, c := range cases {
		delay, retry := p.NextDelay(c.attemptsMade)
		if delay != c.wantDelay || retry != c.wantRetry {
			t.Errorf("NextDelay(%d) = (%v, %v), want (%v, %v)",
				c.attemptsMade, delay, retry, c.wantDelay, c.wantRetry)
		}
	}
}

func TestBackoffPolicy_CustomSchedule(t *testing.T) {
	p := NewBackoffPolicy([]time.Duration{time.Second, 2 * time.Second})

	if got := p.MaxAttempts(); got != 3 {
		t.Fatalf("MaxAttempts = %d, want 3", got)
	}
	if d, ok := p.NextDelay(1); d != time.Second || !ok {
		t.Errorf("NextDelay(1) = (%v, %v), want (1s, true)", d, ok)
	}
	if d, ok := p.NextDelay(2); d != 2*time.Second || !ok {
		t.Errorf("NextDelay(2) = (%v, %v), want (2s, true)", d, ok)
	}
	if _, ok := p.NextDelay(3); ok {
		t.Errorf("NextDelay(3) retry = true, want false (exhausted)")
	}
}

func TestBackoffPolicy_EmptyFallsBackToDefault(t *testing.T) {
	p := NewBackoffPolicy([]time.Duration{})
	if len(p.Schedule) != len(DefaultBackoffSchedule) {
		t.Fatalf("empty schedule did not fall back to default: got %d entries", len(p.Schedule))
	}
}

func TestBackoffPolicy_NegativeAttemptsTreatedAsImmediate(t *testing.T) {
	p := NewBackoffPolicy(nil)
	if d, ok := p.NextDelay(-1); d != 0 || !ok {
		t.Errorf("NextDelay(-1) = (%v, %v), want (0, true)", d, ok)
	}
}
