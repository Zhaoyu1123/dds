package job

import "time"

// OnceSchedule represent job will only schedule one times.
type OnceSchedule struct {
	Delay time.Time
}

// Next returns the next time this schedule is activated, greater than the given
// time. If no return the zero time.
func (os *OnceSchedule) Next(t time.Time) time.Time {
	if os.Delay.After(t) {
		return os.Delay
	}
	return time.Time{}
}
