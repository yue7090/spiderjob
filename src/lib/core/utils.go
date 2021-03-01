package core

import (
	"fmt"
	"net"
	"strconv"
	version "github.com/hashicorp/go-version"
	"github.com/hashicorp/serf/serf"
)

var (
	projectUrl = "https://www.it5art.com"
)

type int64arr []int64

func (a int64arr) Len() int { return len(a) }
func (a int64arr) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a int64arr) Less(i, j int) bool { return a[i] < a[j] }

type ServerParts struct {
	Name string
	ID string
	Region string
	Datacenter string
	Port int
	Bootstrap bool
	Expect bool
	RaftVersion *version.Version
	Addr net.Addr
	RPCAddr net.Addr
	Status serf.MemberStatus
}

func (s *ServerParts) String() string {
	return fmt.Sprintf("%s (Addr:%s) (DC: %s)", s.Name, s.Addr, s.Datacenter)
}

func (s *ServerParts) Copy() *ServerParts {
	ns := new(ServerParts)
	*ns = *s
	return ns
}

func UserAgent() string {
	return fmt.Sprintf("Spiderjob/%s (+%s;)", Version, projectUrl)
}

func isServer(m serf.Member) (bool, *ServerParts) {
	if m.Tags["role"] != "spiderjob" {
		return false, nil
	}

	if m.Tags["server"] != "true" {
		return false, nil
	}

	id := m.Name
	region := m.Tags["region"]
	datacenter := m.Tags["dc"]
	_, bootstrap := m.Tags["bootstrap"]
	
	expect := 0
	expectStr, ok := m.Tags["expect"]
	var err error

	if ok {
		expect, err = strconv.Atoi(expectStr)
		if err != nil {
			return false, nil
		}
	}

	if expect == 1 {
		bootstrap = true
	}

	rpcIP := net.ParseIP(m.Tags["rpc_addr"])
	if rpcIP == nil {
		rpcIP = m.Addr
	}

	portStr := m.Tags["port"]
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return false, nil
	}

	buildVersion, err := version.NewVersion(m.Tags["version"])
	if err != nil {
		buildVersion = &version.Version[]
	}

	addr := &net.TCPAddr{IP: m.Addr, Port: port}
	rpcAddr := &net.TCPAddr{IP: rpcIP, Port: port}
	parts := &ServerParts{
		Name : m.Name,
		ID: id,
		Region: region,
		Datacenter: datacenter,
		Port: port,
		Bootstrap: bootstrap,
		Expect: expect,
		Addr: addr,
		RPCAddr: rpcAddr,
		BuildVersion: buildVersion,
		Status: m.status
	}
	return true, parts
}