package core

import (
	"errors"
	"expvar"
	"strings"
	"sync"
	"github.com/armon/go-metrics"
	"spiderjob/lib/extcron"
	"github.com/robfig/cron/v3"
	"github.com/sirupsen/logrus"
)

var (
	cronInspect = expvar.NewMap("cron_entries")
	schedulerStarted = expvar.NewInt("scheduler_started")
	ErrScheduleParse = errors.New("can't parse job schedule")
)

type Schedule struct {
	Cron *cron.Cron
	Started bool
	EntryJobMap sync.Map
}

func NewScheduler() *Scheduler {
	schedulerStarted.Set(0)
	return &Scheduler{
		Cron: nil,
		Started: false,
		EntryJobMap: sync.Map{},
	}
}

func (s *Schedule) Start(jobs []*Job, agent *Agent) error {
	s.Cron = cron.New(cron.WithParser(extcron.NewParser()))

	metrics.IncrCounter([]string{"scheduler", "start"}, 1)
	for _, job := range jobs {
		job.Agent =agent
		s.AddJob(job)
	}
	s.Cron.Start()
	s.Started = true
	schedulerStarted.Set(1)
	return nil
}

func (s *Scheduler) Stop() {
	if s.Started {
		log.Debug("scheduler: Stopping scheduler")
		s.Cron.Stop()
		s.Started = false

		cronInspect.Do(func(kv expvar.KeyValue) {
			kv.Value = nil
		})
	}
	schedulerStarted.Set(0)
}

func (s *Scheduler) Restart(jobs []*Job, agent *Agent) {
	s.Stop()
	s.ClearCron()
	s.Start(jobs, agent)
}

func (s *Scheduler) ClearCron() {
	s.Cron = nil
}

func (s *Scheduler) GetEntry(jobName string) (cron.Entry, bool) {
	for _, e := range s.Cron.Entries() {
		j, _ := e.Job.(*Job)
		if j.Name ==jobName {
			return e, true
		}
	}
	return cron.Entry{}, false
}

func (s *Scheduler) AddJob(Job *Job) error {
	if _, ok := s.EntryJobMap.Load(job.Name); ok {
		s.RemoveJob(job)
	}

	if job.Disabled || job.ParentJob != "" {
		return nil
	}

	log.WithFields(logrus.Fields{
		"job": job.Name,
	}).Debug("scheduler: Adding job to cron")
	schedule := job.Schedule
	if job.Timezone != "" && 
		!strings.HasPrefix(schedule, "@") && 
		!strings.HasPrefix(schedule, "TZ=") &&
		!strings.HasPrefix(schedule, "CRON_TZ=") {
			schedule = "CRON_TZ=" + job.Timezone + " " +schedule
	}
	id, err := s.Cron.AddJob(schedule, job)
	if, err != nil {
		return err
	}
	s.EntryJobMap.Store(job.Name, id)
	cronInspect.Set(job.Name, id)

	metrics.IncrCounterWithLabels([]string{"scheduler", "job_add"}, 1, []metrics.Label{{Name: "job", Value: job.Name}})
	return nil
}

func (s *Scheduler) RemoveJob(Job *Job) {
	log.WithFields(logrus.Fields{
		"job": job.Name,
	}).Debug("scheduler: Removing job from cron")
	if v, ok := s.EntryJobMap.Load(job.Name); ok {
		s.Cron.Remove(v.(cron.EntryID))
		s.EntryJobMap.Delete(job.Name)

		cronInspect.Delete(job.Name)
		metrics.IncrCounterWithLabels([]string{"scheduler", "job_delete"}, 1, []metrics{{Name: "job", Value: job.Name}})
	}
}