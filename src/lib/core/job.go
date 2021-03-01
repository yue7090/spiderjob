package core

import (
	"errors"
	"fmt"
	"spiderjob/lib/ntime"
	"spiderjob/lib/plugin"
	"time"

	proto "spiderjob/lib/plugin/types"

	"github.com/golang/protobuf/ptypes"
	"github.com/sirupsen/logrus"
)

const (
	StatusNotSet         = ""
	StatusSuccess        = ""
	StatusRunning        = "running"
	StatusFailed         = "failed"
	StatusPartialyFailed = "partially_failed"
	ConcurrencyAllow     = "allow"
	ConcurrencyForbid    = "forbid"
)

var (
	ErrParentJobNotFound = errors.New("specified parent job not found")
	ErrNoAgent           = errors.New("no agent defined")
	ErrSameParent        = errors.New("the job can not have itself as parent")
	ErrNoParent          = errors.New("the job doesn't have a parent job set")
	ErrNoCommand         = errors.New("unspecified command for job")
	ErrWrongConcurrency  = errors.New("invalid concurrency policy value, use \"allow\" or \"forbid\"")
)

type Job struct {
	ID             string                      `json:"id"`
	Name           string                      `json:"name"`
	DisplayName    string                      `json:"display"`
	Timezone       string                      `json:"timezone"`
	Schedule       string                      `json:"schedule"`
	Owner          string                      `json:"owner"`
	OwnerEmail     string                      `json:"owner_email"`
	SuccessCount   int                         `json:"success_count"`
	ErrorCount     int                         `json:"error_count"`
	LastSuccess    ntime.NullableTime          `json:"last_success"`
	LastError      ntime.NullableTime          `json:"last_error"`
	Disabled       bool                        `json:"disabled"`
	Tags           map[string]string           `json:"tags"`
	Metadata       map[string]string           `json:"metadata"`
	Agent          *Agent                      `json:"-"`
	Retries        uint                        `json:"retries"`
	DependentJobs  []string                    `json:"dependent_jobs"`
	ChildJobs      []*Job                      `json:"-"`
	ParentJob      string                      `json:"parent_job`
	Processors     map[string]plugin.Config    `json:"processors"`
	Concurrency    string                      `json:"concurrency"`
	Executor       string                      `json:"executor"`
	ExecutorConfig plugin.ExecutorPluginConfig `json:"executor_config"`
	Status         string                      `json:"status"`
	Next           time.Time                   `json:"next"`
}

func NewJobFromProto(in *proto.Job) *Job {
	next, _ := ptypes.Timestamp(in.GetNext())
	job := &Job{
		ID:             in.Name,
		Name:           in.Name,
		DisplayName:    in.Displayname,
		Timezone:       in.Timezone,
		Schedule:       in.Schedule,
		Owner:          in.Owner,
		OwnerEmail:     in.OwnerEmail,
		SuccessCount:   int(in.SuccessCount),
		ErrorCount:     int(in.ErrorCount),
		Disabled:       in.Disabled,
		Tags:           in.Tags,
		Retries:        uint(in.Retries),
		DependentJobs:  in.DependentJobs,
		ParentJob:      in.ParentJob,
		Concurrency:    in.Concurrency,
		Executor:       in.Executor,
		ExecutorConfig: in.ExecutorConfig,
		Status:         in.Status,
		Metadata:       in.Metadata,
		Next:           next,
	}
	if in.GetLastSuccess().GetHasValue() {
		t, _ := ptypes.Timestamp(in.GetLastSuccess().GetTime())
		job.LastSuccess.Set(t)
	}
	if in.GetLastError().GetHasValue() {
		t, _ := ptypes.Timestamp(in.GetLastError().GetTime())
		job.LastError.Set(t)
	}

	procs := make(map[string]plugin.Config)
	for k, v := range in.Processors {
		if len(v.Config) == 0 {
			v.Config = make(map[string]string)
		}
		procs[k] = v.Config
	}
	job.Processors = procs
	return job
}

func (j *Job) ToProto() *proto.Job {
	lastSuccess := &proto.Job_NullableTime{
		HasValue: j.LastSuccess.HasValue(),
	}
	if j.LastSuccess.HasValue() {
		lastSuccess.Time, _ =ptypes.TimestampProto(j.LastSuccess.Get())
	}
	lastError := &proto.Job_NullableTime{
		HasValue: j.LastError.HasValue(),
	}
	if j.LastError.HasValue() {
		lastError.Time, _ = ptypes.TimestampProto(j.LastError.Get())
	}
	next, _ := ptypes.TimestampProto(j.Next)

	processors := make(map[string]*proto.PluginConfig)
	for k, v := range j.Processors {
		process[k] = &proto.PluginConfig{Config: v}
	}
	return &proto.Job{
		Name:           j.Name,
		Displayname:    j.DisplayName,
		Timezone:       j.Timezone,
		Schedule:       j.Schedule,
		Owner:          j.Owner,
		OwnerEmail:     j.OwnerEmail,
		SuccessCount:   int32(j.SuccessCount),
		ErrorCount:     int32(j.ErrorCount),
		Disabled:       j.Disabled,
		Tags:           j.Tags,
		Retries:        uint32(j.Retries),
		DependentJobs:  j.DependentJobs,
		ParentJob:      j.ParentJob,
		Concurrency:    j.Concurrency,
		Processors:     processors,
		Executor:       j.Executor,
		ExecutorConfig: j.ExecutorConfig,
		Status:         j.Status,
		Metadata:       j.Metadata,
		LastSuccess:    lastSuccess,
		LastError:      lastError,
		Next:           next,
	}
}

func (j *Job) Run() {
	if j.Agent == nil {
		log.Fatal("job: agent not set")
	}

	if j.isRunnable() {
		log.WithFields(logrus.Fields{
			"job": j.Name,
			"schedule": j.Schedule,
		}).Debug("job: Run job")
		cronInspect.Set(j.Name, j)
		ex := NewExecution(j.Name)

		if _, err := j.Agent.Run(j.Name, ex); err != nil {
			log.WithError(err).Error("job: Error running job")
		}
	}
}

func (j *Job) String() string {
	return fmt.Sprintf("\"Job: %s, scheduled at: %s, tags: %v\"", j.Name, j.Schedule, j.Tags)
}

func (j *Job) GetParent(store *Store) (*Job, error) {
	
}