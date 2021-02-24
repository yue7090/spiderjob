package plugin

import (
	"github.com/hashicorp/go-plugin"
)


const (
	ProcessorPluginName = "processor"
	ExecutorPluginName  = "executor"
)

var Handshake = plugin.HandshakeConfig{
	ProtocolVersion: 1,
	MagicCookieKey: "SPIDERJOB_PLUGIN_MAGIC_COOKIE",
	MagicCookieValue: "1234567"
}

type ServeOpts struct {
	Processor Processor
	Executor Executor
}

func Serve(opts *ServeOpts) {
	plugin.Serve(&plugin.ServeConfig{
		HandshakeConfig: Handshake,
		Plugins: pluginMap(opts),
	})
}

func pluginMap(opts *ServeOpts) map[string]plugin.Plugin {
	return map[string]plugin.Plugin{
		"processor": &ProcessorPlugin{Processor: opts.Processor},
	}
}