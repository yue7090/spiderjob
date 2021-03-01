package core

import (
	"crypto/tls"
)

func WithPlugins(plugins Plugins) AgentOption {
	return func(agent *Agent) {
		agent.ProcessorPlugins = plugins.Processors
		agent.ExecutorPlugins = plugins.Executors
	}
}

func WithTransportCredentials(tls *tls.Config) AgentOption {
	return func(agent &Agent) {
		agent.TLSConfig = tls
	}
}

func WithStore(store Storage) AgentOption {
	return func(agent *Agent) {
		agent.Store = store
	}
}