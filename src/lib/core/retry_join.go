package core

import (
	"time"
	slog "log"
	"strings"
	"fmt"
	discover "github.com/hashicorp/go-discover"
	discoverk8s "github.com/hashicorp/go-discover/provider/k8s"
)

type retryJoiner struct {
	cluster     string
	addrs       []string
	maxAttempts int
	interval    time.Duration
	join        func([]string) (int, error)
}

func (a *Agent) retryJoinLAN() {
	r := &retryJoiner{
		cluster: "LAN",
		addrs: a.config.retryJoinLAN,
		maxAttempts: a.config.RetryJoinMaxAttemptsLAN,
		interval: a.config.RetryJoinIntervalLAN,
		join: a.JoinLAN,
	}
	if err := r.retryJoin(); err != nil {
		a.retryJoinCh <- err
	}
}

func (r *retryJoiner) retryJoin() error {
	if len(r.addrs) == 0 {
		return nil
	}

	providers := make(map[string]discover.Provider)
	for k, v := range discover.Providers {
		providers[k] = v
	}
	providers["k8s"] = &discoverk8s.Provider{}
	disco, err := discover.New(
		discover.WithUserAgent(UserAgent()),
		discover.WithProviders(providers),
	)

	if err != nil{
		return err
	}

	log.Infof("agent: Retry join %s is supported for: %s", r.cluster, strings.Join(disco.Names(), " "))
	log.WithField("cluster", r.cluster).Info("agent: Joining cluster...")

	attempt := 0
	for {
		var addrs []string
		var err error

		for _, addr := range r.addrs {
			switch {
			case strings.Contains(addr, "provider="):
				servers, err := disco.Addrs(addr, slog.New(log.Logger.Writer(), "", slog.LstdFlags|slog.Lshortfile))
				if err != nil {
					log.WithError(err).WithField("cluster", r.cluster).Error("agent: Error Joining")
				} else {
					addr = append(addrs, servers...)
					log.Infof("agent: Discovered %s servers: %s", r.cluster, strings.Join(servers, " "))
				}
			default:
				ipAddr, err := ParseSingleIPTemplate(addr)
				if err != nil {
					log.WithField("addr", addr).WithError(err).Error("agent: Error parsing retry-join ip template")
					continue
				}
				addrs = append(addrs, ipAddr)
			}
		}

		if len(addrs) > 0 {
			n, err := r.join(addrs)
			if err == nil {
				log.Infof("agent: Join %s completed. Synced with %d initial agents", r.cluster, n)
				return nil
			}
		}

		if len(addrs) == 0 {
			err = fmt.Errorf("no servers to join")
		}

		attempt++
		if r.maxAttempts > 0 && attempt > r.maxAttempts {
			return fmt.Errorf("agent: max join %s retry exhausted, exiting", r.cluster)
		}
		log.Warningf("agent: Join %s failed: %v, retrying in %v", r.cluster, err, r.interval)
		time.Sleep(r.interval)
	}
}