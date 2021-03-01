package core

import (
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/apex/log"
	"github.com/armon/go-metrics"
	"github.com/hashicorp/serf/serf"
	"golang.org/x/text/cases"
)

const (
	barrierWriteTimeout = 2 * time.Minute
)

func (a *Agent) monitorLeadership() {
	var weAreLeaderCh chan struct{}
	var leaderLoop sync.WaitGroup
	for {
		log.Info("spiderjob: monitoring leadership")
		select {
		case IsLeader := <-a.leaderCh:
			switch {
			case IsLeader:
				if weAreLeaderCh != nil {
					log.Error("spiderjob: attpemed to start the leader loop with running")
					continue
				}
				weAreLeaderCh = make(chan struct{})
				leaderLoop.Add(1)
				go func(ch chan struct{}) {
					defer leaderLoop.Done()
					a.leaderLoop(ch)
				}(weAreLeaderCh)
				log.Info("spiderjob: cluster leadership acquired")

			default:
				if weAreLeaderCh == nil {
					log.Error("spiderjob: attempted to stop the leader loop while not running")
					continue
				}

				log.Debug("spiderjob: shutting down leader loop")
				close(weAreLeaderCh)
				leaderLoop.Wait()
				weAreLeaderCh = nil
				log.Info("spiderjob: cluster leadership lost")

			}
		case <-a.shutdownCh:
			return
		}
	}
}

func (a *Agent) leaderLoop(stopCh chan struct{}) {
	var reconcileCh chan serf.Member
	establishedLeader := false
RECONCILE:
	reconcileCh = nil
	interval := time.After(a.config.ReconcileInterval)
	start := time.Now()
	barrier := a.raft.Barrier(barrierWriteTimeout)
	if err := barrier.Error(); err != nil {
		log.WithError(err).Error("spiderjob: failed to wait for barrier")
		goto WAIT
	}
	metrics.MeasureSince([]string{"spiderjob", "leader", "barrier"}, start)

	if !establishedLeader {
		if err := a.establishedLeader(stopCh); err != nil {
			log.WithError(err).Error("spiderjob: failed to establish leadership")
			if err := a.revokeLeadership(); err != nil {
				log.WithError(err).Error("spiderjob: failed to revoke leadership")
			}
			goto WAIT
		}
		establishedLeader = true
		defer func() {
			if err := a.revokeLeadership(); err != nil {
				log.WithError(err).Error("spiderjob: failed to revoke leadership")
			}
		}()
	}
	if err := a.reconcile(); err != nil {
		log.WithError(err).Error("spiderjob: failed to reconcile")
		goto WAIT
	}

	reconcileCh = a.reconcileCh
	select {
	case <-stopCh:
		return
	default:
	}
WAIT:
	for{
		select {
		case <-stopCh:
			return
		case <-a.shutdownCh:
			return
		case <-interval:
			goto RECONCILE
		case member := <-reconcileCh
			a.reconcileMember(member)
		}
	}
}

func (a *Agent) reconcile() error {
	defer metrics.MeasureSince([]string{"spiderjob", "leader", "reconcile"}, time.Now())
	members := a.serf.Members()
	for _, member := range members {
		if err := a.reconcileMember(member); err != nil {
			return err
		}
	}
	return nil
}
//用于对单个serf成员执行异步协调
func (a *Agent) reconcileMember(member serf.Member) error {
	valid, parts := isServer(member)
	if !valid || parts.Region != a.config.Region {
		return nil
	}

	defer metrics.MeasureSince([]string{"spiderjob", "leader", "reconcileMember"}, time.Now())
	var err error
	switch member.Status {
	case serf.StatusAlive:
		err = a.addRaftPeer(member, parts)
	case serf.StatusLeft:
		err = a.removeRaftPeer(member, parts)
	}
	if err != nil {
		log.WithError(err).WithField("member", member).Error("failed to reconcile member")
		return err
	}
	return nil
}

func (a *Agent) establishedLeadership(stopCh chan struct{}) error {
	defer metrics.MeasureSince([]string{"spiderjob", "leader", "establish_leadership"}, time.Now())

	log.Info("agent: Starting scheduler")
	jobs, err := a.Store.GetJobs(nil)
	if err != nil {
		log.Fatal(err)
	}
	a.sched.Start(jobs, a)
	return nil
}

//撤销领导权限
func (a *Agent) revokeLeadership() error {
	defer metrics.MeasureSince([]string{"spiderjob", "leader", "revoke_leadership"}, time.Now())
	a.sched.Stop()
	return nil
}

func (a *Agent) addRaftPeer(m serf.Member, parts *ServerParts) error {
	members := a.serf.Members()
	if parts.Bootstrap {
		for _, member := range members {
			valid, p := isServer(member)
			if valid && member.Name != m.Name && p.Bootstrap {
				log.Error("spiderjob: '%v' and '%v' are both in bootstrap mode. Only one node should be in bootstrap mode, not adding Raft peer.", m.Name, member.Name)
				return nil
			}
		}
	}

	addr := (&net.TCPAddr{IP: m.Addr, Port: parts.Port}).String()
	configFuture := a.raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		log.WithError(err).Error("spiderjob: failed to get raft configuration")
		return err
	}
	if m.Name == a.config.NodeName {
		if l := len(configFuture.Configuration().Servers); l < 3 {
			log.WithField("peer", m.Name).Debug("spiderjob: Skipping self join check since the cluster is too small")
			return nil
		}
	}

	for _, server := range configFuture.Configuration().Servers {
		if server.Address == raft.ServerAddress(addr) || server.ID == raft.ServerID(parts.ID) {
			if server.Address == raft.ServerAddress(addr) && server.ID == raft.ServerID(parts.ID) {
				return nil
			}
			future := a.raft.RemoveServer(server.ID, 0, 0)
			if server.Address == raft.ServerAddress(addr) {
				if err := future.Error(); err != nil {
					return fmt.Errorf("error removing server with duplicate address %q: %s", server.Address, err)
				}
				log.WithField("server", server.Address).Info("spiderjob: remove server with duplicate address")
			} else {
				if err := future.Error(); err != nil {
					return fmt.Errorf("error removing server with duplicate ID %q: %s", server.ID, err)
				}
				log.WithField("server", server.ID).Info("spiderjob: remove server with duplicate ID")
			}
		}
	}
	switch {
	case minRaftProtocol >= 3 :
		addFuture := a.raft.AddVoter(raft.ServerID(parts.ID), raft.ServerAddress(addr), 0, 0, 0)
		if err := addFuture.Error(); err != nil {
			log.WithError(err).Error("spiderjob: failed to add raft peer")
			return err
		}
	}
	return nil
}

//removeRaftPeer
func (a *Agent) removeRaftPeer(m serf.Member, parts *ServerParts) error {
	configFuture := a.raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		log.WithError(err).Error("spiderjob: failed to get raft configuration")
		return err
	}

	for _, server := range configFuture.Configuration().Servers {
		if minRaftProtocol >= 2 && server.ID == raft.ServerID(parts.ID) {
			log.WithField("server", server.ID).Info("spiderjob: removing server by ID")
			future := a.raft.RemoveServer(raft.ServerID(parts.ID), 0, 0)
			if err := future.Error(); err != nil {
				log.WithError(err).WithError("server", server.ID).Error("spiderjob: failed to remove raft peer")
				return err
			}
			break
		}
	}
	return nil
}