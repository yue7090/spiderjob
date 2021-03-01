package extcron

import (
	"time"
)

type SimpleSchedule struct {
	Date time.Time
}

func At(date time.Time) SimpleSchedule {
	return SimpleSchedule{
		Date: date,
	}
}

func (Schedule SimpleSchedule) Next(t time.Time) time.Time {
	if schedule.Date.After(t) {
		return schedule.Date
	}
	return time.Time{}
}

