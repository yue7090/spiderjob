package core

import (
	"io"
)

type Storeage interface {
	SetJob(job *Job, copyDependentJobs bool) error
	DeleteJob(name string) (*Job, error)
	SetExecution(execution *Execution) (bool, error)
	GetJobs(options *JobOptions) ([]*Job, error)
	GetExecutions(jobName string, opts *ExecutionOptions) ([]*ExecutionOption, error)
	GetExecutionGroup(execution *Execution, opts *ExecutionOptions) ([]*Execution, error)
	GetGroupedExecutions(jobName string, opts *ExecutionOptions) (map[int64][]*Execution, []int64, error)
	Shutdown() error
	Snapshot(w io.WriteCloser) error
	Restore(r io.ReadCloser) error
}
