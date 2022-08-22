package job

import (
	"context"
	"encoding/json"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/google/uuid"
)

var backend *redis.ClusterClient

func SetBackend(cli *redis.ClusterClient) {
	backend = cli
}

func Get(id string) (*Job, error) {
	result, err := backend.Get(context.Background(), id).Result()
	if err != nil {
		return nil, nil
	}

	var j Job
	if err := json.Unmarshal([]byte(result), &j); err != nil {
		return nil, err
	}
	return &j, nil
}

func GetBytes(id string) ([]byte, error) {
	result, err := backend.Get(context.Background(), id).Bytes()
	if err != nil {
		return nil, nil
	}

	return result, nil
}

// Job is an instance for Worker process.
type Job struct {
	// Unique Id is used for query and delete.
	Id string

	// specific of job execute time describe.
	ExecuteTimeSpec string

	// next expected execution time.
	NextExecuteTime time.Time
	// last execution time.
	LastExecuteTime time.Time

	// represent the max difference between actual execution time and expected execution time.
	DitherWindow int

	// max retry count, default is 3.
	RetryCount int
	// default backoff start at 1.
	RetryBackoff int

	Data []byte

	State StateType `json:"state"`

	// Used for log which bucket store this task, only for trace.
	Partition string `json:"partition"`

	// represent the life cycle of the last execution.
	CreateAt  string `json:"create_at"`
	ReceiveAt string `json:"receive_at"`
	StartAt   string `json:"start_at"`
	DeleteAt  string `json:"delete_at"`
	FinishAt  string `json:"finish_at"`

	// ExecuteTimes++ when job finished.
	ExecuteTimes int `json:"execute_times"`

	// Errors length may large then one when task execute times large then one.
	Errors []string `json:"error"`
}

// StateType represent job current state.
type StateType string

const (
	// Pending - initial state of a task
	Pending StateType = "PENDING"
	// Received - when task is received by a worker
	Received StateType = "RECEIVED"
	// Started - when the worker start processing the task
	Started StateType = "STARTED"
	// Retry - when failed task has been scheduled for retry
	Retry StateType = "RETRY"
	// Success - when the task is processed successfully
	Success StateType = "SUCCESS"
	// Failure - when the task is processed failed
	Failure StateType = "FAILURE"
	// Deleted - when the task is deleted
	Deleted StateType = "REMOVED"
	// Unknown - task state unknown
	Unknown StateType = "UNKNOWN"
)

type FuncOption func(job *Job)

// WithPartition used to record in which bucket the task is stored.
func WithPartition(partition string) FuncOption {
	return func(job *Job) {
		job.Partition = partition
	}
}

// WithError used to record job execution failed error.
func WithError(error string) FuncOption {
	return func(job *Job) {
		job.Errors = append(job.Errors, error)
	}
}

// WithRetryCount used to set failed retries, default is 3.
func WithRetryCount(count int) FuncOption {
	return func(job *Job) {
		job.RetryCount = count
	}
}

// Create a job instance and return an error.
func Create(spec string, data []byte, options ...FuncOption) (Job, error) {
	schedule, err := Parse(spec)
	if err != nil {
		return Job{}, err
	}

	j := &Job{
		Id:              "job-" + uuid.New().String(),
		ExecuteTimeSpec: spec,
		NextExecuteTime: schedule.Next(time.Now()),
		RetryCount:      3,
		RetryBackoff:    1,
		Data:            data,
	}

	for _, option := range options {
		option(j)
	}

	return *j, nil
}

// SetState used to set job current state.
func (s *Job) SetState(ctx context.Context, state StateType, options ...FuncOption) error {
	newState := &Job{State: state}
	for _, option := range options {
		option(newState)
	}
	switch state {
	case Pending:
		newState.CreateAt = time.Now().Local().String()
	case Received:
		newState.ReceiveAt = time.Now().Local().String()
	case Started:
		newState.StartAt = time.Now().Local().String()
	case Retry:
	case Success, Failure:
		newState.FinishAt = time.Now().Local().String()
		newState.ExecuteTimes = s.ExecuteTimes + 1
	case Deleted:
		newState.DeleteAt = time.Now().Local().String()
	}

	s.merge(newState)

	bt, err := json.Marshal(s)
	if err != nil {
		return err
	}
	expiration := s.NextExecuteTime.Sub(time.Now().Local())*time.Second + 7*24*time.Hour
	return backend.Set(ctx, s.Id, string(bt), expiration).Err()
}

// merge new state to current state.
func (s *Job) merge(newJob *Job) {
	if newJob.State != "" {
		s.State = newJob.State
	}
	if newJob.Partition != "" {
		s.Partition = newJob.Partition
	}
	if newJob.CreateAt != "" {
		s.CreateAt = newJob.CreateAt
	}
	if newJob.ReceiveAt != "" {
		s.ReceiveAt = newJob.ReceiveAt
	}
	if newJob.StartAt != "" {
		s.StartAt = newJob.StartAt
	}
	if newJob.DeleteAt != "" {
		s.DeleteAt = newJob.DeleteAt
	}
	if newJob.FinishAt != "" {
		s.FinishAt = newJob.FinishAt
	}
	if newJob.ExecuteTimes != 0 {
		s.ExecuteTimes = newJob.ExecuteTimes
	}
	if newJob.Errors != nil {
		s.Errors = append(s.Errors, newJob.Errors...)
	}
}
