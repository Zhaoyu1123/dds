package workerpool

// WorkerPool provides a pool for goroutines
type WorkerPool interface {
	// Schedule try to acquire pooled worker goroutine to execute the specified task,
	// this method would block if no worker goroutine is available
	Schedule(task func())

	// ScheduleAlways try to acquire pooled worker goroutine to execute the specified task first,
	// but would not block if no worker goroutine is available. A temp goroutine will be created for task execution.
	ScheduleAlways(task func())

	// ScheduleAuto executes Schedule logic first.
	// If Schedule failed, then executes ScheduleAlways logic.
	ScheduleAuto(task func())

	// Close waiting for all goroutine exit.
	// If the goroutine is execute an never quit task, Close will blocking.
	Close()
}
