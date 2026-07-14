package service

import "time"

// DefaultBackoffSchedule is the wait applied before each retry that follows a
// failed attempt. The first attempt is made immediately; if it fails we wait
// Schedule[0] before the second attempt, Schedule[1] before the third, and so
// on. Once the schedule is exhausted the event is dead-lettered (status
// "failed") with no further retries.
var DefaultBackoffSchedule = []time.Duration{
	30 * time.Second,
	2 * time.Minute,
	10 * time.Minute,
	1 * time.Hour,
}

// BackoffPolicy computes retry timing from a fixed, capped schedule. It holds
// no state and is safe for concurrent use, which keeps the delivery logic in
// the dispatcher trivial to reason about and unit test.
type BackoffPolicy struct {
	Schedule []time.Duration
}

// NewBackoffPolicy returns a policy using the given schedule, or the default
// schedule when none is provided. Passing a custom (short) schedule lets tests
// exercise retry behaviour without waiting real minutes.
func NewBackoffPolicy(schedule []time.Duration) BackoffPolicy {
	if len(schedule) == 0 {
		schedule = DefaultBackoffSchedule
	}
	return BackoffPolicy{Schedule: schedule}
}

// MaxAttempts is the total number of delivery attempts before an event is
// dead-lettered: one immediate attempt plus one per schedule entry.
func (p BackoffPolicy) MaxAttempts() int {
	return len(p.Schedule) + 1
}

// NextDelay reports how long to wait before the next attempt, and whether a
// next attempt should happen at all, given the number of attempts already
// made.
//
//	attemptsMade == 0 -> (0, true)          first attempt, immediate
//	attemptsMade == 1 -> (Schedule[0], true) wait before the 2nd attempt
//	...
//	attemptsMade == len(Schedule)   -> (0, false) schedule exhausted, give up
//
// A false second return means the event has reached its final attempt and
// should be marked failed.
func (p BackoffPolicy) NextDelay(attemptsMade int) (time.Duration, bool) {
	if attemptsMade <= 0 {
		return 0, true
	}
	idx := attemptsMade - 1
	if idx >= len(p.Schedule) {
		return 0, false
	}
	return p.Schedule[idx], true
}
