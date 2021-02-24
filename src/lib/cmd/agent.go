package cmd

import (
	"errors"
	"expvar"
	"sync"
	"time"
)


const (
	raftTimeout = 30 * time.Second
	raftLogCacheSize = 512
	minRaftProtocol = 3
)

var (
	exNode = expvar.NewString("node")
	ErrLeaderNotFound = errors.New("no member leader found in memberlist")
	ErrNosuitableServer = errors.New("no suitable server found to send the request, aborting")
	runningExecutions sync.Map
)