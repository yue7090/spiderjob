package core

import (
	"time"
)

type retryJoiner struct {
	cluster     string
	addrs       []string
	maxAttempts int
	interval    time.Duration
	join        func([]string) (int, error)
}
