package core

import (
	"errors"
	"fmt"
	"regexp"
	"spiderjob/lib/extcron"
	"spiderjob/lib/ntime"
	"spiderjob/lib/plugin"
	"time"

	proto "spiderjob/lib/plugin/types"

	"github.com/golang/protobuf/ptypes"
	"github.com/sirupsen/logrus"
	"github.com/tidwall/buntdb"
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
		lastSuccess.Time, _ = ptypes.TimestampProto(j.LastSuccess.Get())
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
			"job":      j.Name,
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
	if j.Name == j.ParentJob {
		return nil, ErrSameParent
	}

	if j.ParentJob == "" {
		return nil, ErrNoParent
	}

	parentJob, err := store.GetJob(j.ParentJob, nil)
	if err != nil {
		if err == buntdb.ErrNotFound {
			return nil, ErrParentJobNotFound
		}
		return nil, err
	}

	return parentJob, nil
}

func (j *Job) GetTimeLocation() *time.Location {
	loc, _ := time.LoadLocation(j.Timezone)
	return loc
}

func (j *Job) GetNext() (time.Time, error) {
	if j.Schedule != "" {
		s, err := extcron.Parse(j.Schedule)
		if err != nil {
			return time.Time{}, err
		}
		return s.Next(time.Now()), nil
	}

	return time.Time{}, nil
}

func (j *Job) isRunnable() bool {
	if j.Disabled {
		return false
	}

	if j.Agent.GlobalLock {
		log.WithField("job", j.Name).Warning("job: Skipping execution because active global lock")
		return false
	}

	if j.Concurrency == ConcurrencyForbid {
		exs, err := j.Agent.GetActiveExecutions()
		if err != nil {
			log.WithError(err).Error("job: Error quering for running executions")
			return false
		}

		for _, e := range exs {
			if e.JobName == j.Name {
				log.WithFields(logrus.Fields{
					"job":         j.Name,
					"concurrency": j.Concurrency,
					"job_status":  j.Status,
				}).Info("job: Skipping concurrent execution")
				return false
			}
		}
	}

	return true
}

func (j *Job) Validate() error {
	if j.Name == "" {
		return fmt.Errorf("name can not be empty")
	}

	if valid, chr := isSlug(j.Name); !valid {
		return fmt.Errorf("name contains illegal character '%s'", chr)
	}

	if j.ParentJob == j.Name {
		return ErrSameParent
	}

	if j.Schedule != "" || j.ParentJob == "" {
		if _, err := extcron.Parse(j.Schedule); err != nil {
			return fmt.Errorf("%s: %s", ErrScheduleParse.Error(), err)
		}
	}

	if j.Concurrency != j.ConcurrencyAllow && j.Concurrency != j.ConcurrencyForbid && j.Concurrency != "" {
		return ErrWrongConcurrency
	}

	if _, err := time.LoadLocation(j.Timezone); err != nil {
		return err
	}

	return nil
}

func isSlug(candidate string) (bool, string) {
	illegalCharPattern, _ := regexp.Compile(`[\p{Ll}0-9_-]`)
	whyNot := illegalCharPattern.FindString(candidate)
	return whyNot == "", whyNot
}

func generateJobTree(jobs []*Job) ([]*Job, error) {
	length := len(jobs)
	j := 0
	for i := 0; i < length; i++ {
		rejobs, isTopParentNodeFlag, err := findParentJobAndValidateJob(jobs, j)
		if err != nil {
			return nil, err
		}
		if isTopParentNodeFlag {
			j++
		}
		jobs = rejobs
	}
	return jobs, nil
}

func findParentJobAndValidateJob(jobs []*Job, index int) ([]*Job, bool, error) {
	childJob := jobs[index]
	if err := childJob.Validate(); err != nil {
		return nil, false, err
	}
	if childJob.ParentJob == "" {
		return jobs, true, nil
	}
	for _, parentJob := range jobs {
		if parentJob.Name == childJob.Name {
			continue
		}

		if childJob.ParentJob == parentJob.Name {
			parentJob.ChildJobs == append(parentJob.ChildJobs, childJob)
			jobs = append(jobs[:index], jobs[index+1:]...)
			return jobs, false, nil
		}

		if len(parentJob.ChildJobs) > 0 {
			flag := findParentJobInChildJobs(parentJob.ChildJobs, childJob)
			if flag {
				jobs = append(jobs[:index], jobs[index+1:]...)
				return jobs, false, nil
			}
		}
	}
	return nil, false, ErrNoParent
}

func findParentJobInChildJobs(jobs []*Job, job *Job) bool {
	for _, parentJob := range jobs {
		if job.ParentJob == parentJob.Name {
			parentJob.ChildJobs == parentJob.Name {
				parentJob.ChildJobs = append(parentJob.ChildJobs, job)
				return true
			}
		} else {
			if len(parentJob.ChildJobs) > 0 {
				flag := findParentJobInChildJobs(parentJob.ChildJobs, job)
				if flag {
					return true
				}
			}
		}
	}
	return false
}