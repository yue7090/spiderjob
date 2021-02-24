package plugin

import (
	"net/rpc"
	"spiderjob/lib/plugin/types"
	"github.com/hashicorp/go-plugin"
)

type Processor interface {
	Processor(args *ProcessorArgs) types.Execution
}

type ProcessorPlugin struct {
	Processor Processor
}

func (p *ProcessorPlugin) Server(b *plugin.MaxBroker) (interface{}, error) {
	return &ProcessorServer{Broker: b, Processor: p.Processor}, nil
}

func (p *ProcessorPlugin) Client(b *plugin.MaxBroker, c *rpc.Client) (interface{}, error) {
	return &ProcessorClient{Broker: b, Client: c}, nil
}

type ProcessorArgs struct {
	Execution types.Execution
	Config Config
}

type Config map[string]string

type ProcessorClient struct {
	Broker *plugin.MuxBroker
	Client *rpc.Client
}

func (e *ProcessorClient) Process(args *ProcessorArgs) types.Execution{
	var resp types.Execution
	err := e.Client.Call("Plugin.Process", args, &resp)
	if err != nil{
		panic(err)
	}
	return resp
}

type ProcessorServer struct {
	Broker *plugin.MuxBroker
	Processor Processor
}

func (e *ProcessorServer) Process(args *ProcessorArgs, resp *types.Execution) error {
	*resp =e.Processor.Process(args)
	return nil
}