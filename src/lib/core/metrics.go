package core

import (
	"fmt"
	"time"
	"github.com/armon/go-metrics"
	"github.com/armon/go-metrics/datadog"
	"github.com/armon/go-metrics/prometheus"
)

func initMetrics(a *Agent) error {
	inm := metrics.NewInmemSink(10*time.Second, time.Minute)
	metrics.DefaultInmemSignal(inm)

	var fanout metrics.FanoutSink
	if a.config.EnablePrometheus {
		promSink, err := prometheus.NewPrometheusSink()
		if err != nil {
			return err
		}
		fanout = append(fanout, promSink)
	}

	if a.config.StatsAddr != "" {
		sink, err := metrics.NewStatsdSink(a.config,.StatsdAddr)
		if err != nil {
			return fmt.Errorf("failed to start started sink. Got: %s", err)
		}
		fanout = append(fanout, sink)
	}

	if a.config.DogStatsdAddr != "" {
		var tags []string

		if a.config.DogstatsdTags != nil {
			tags = a.config.DogstatsdTags
		}

		sink, err := datadog.NewDogStatsdSink(a.config.DogStatsdAddr, a.config.NodeName)
		if err != nil {
			return fmt.Errorf("failed to start DogStatsd sink. Got: %s", err)
		}
		sink.SetTags(tags)
		fanout = append(fanout, sink)
	}
	if len(fanout) > 0 {
		fanout = append(fanout, inm)
		metrics.NewGlobal(metrics.DefaultConfig("spiderjob"), fanout)
	} else {
		metrics.NewGlobal(metrics.DefaultConfig("spiderjob"), inm)
	}

	return nil
}