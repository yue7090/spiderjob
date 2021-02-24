package core

import (
	"crypto/tls"
	"fmt"
	"net"
	"time"

	"github.com/apex/log"
	"github.com/hashicorp/raft"
)

type RaftLayer struct {
	TLSConfig *tls.Config
	ln        net.Listener
}

func NewRaftLayer() *RaftLayer {
	return &RaftLayer{}
}

func NewTLSRafterLayer(tlsConfig *tls.Config) *RaftLayer {
	return &RaftLayer{TLSConfig: tlsConfig}
}

func (t *RaftLayer) Dial(addr raft.ServerAddress, timeout time.Duration) (net.Conn, error) {
	dialer := &net.Dialer{Timeout: timeout}
	var err error
	var conn net.Conn
	if t.TLSConfig != nil {
		log.Debug("doing a TLS dial")
		conn, err = tls.DialWithDialer(dialer, "tcp", string(addr), t.TLSConfig)
	}else{
		conn, err = dialer.Dial("tcp", string(addr))
	}
	return conn, err
}

func (t *RaftLayer) Accept() (net.Conn, error) {
	c, err := t.ln.Accept()
	if err != nil {
		fmt.Println("error accpting: ", err.Error())
	}
	return c, err
}

func (t *RaftLayer) Close() error {
	return t.ln.Close()
}

func (t *RaftLayer) Addr() net.Addr {
	return t.ln.Addr()
}