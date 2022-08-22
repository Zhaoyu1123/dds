package job

import "time"

// Worker used for process job.
type Worker interface {
	// Process job and return with an error.
	Process(j Job) error
}

// Schedule describes a job's execute cycle.
type Schedule interface {
	// Next returns the next activation time, later than the given time.
	// Next is invoked initially, and then each time the job is run.
	Next(time.Time) time.Time
}
