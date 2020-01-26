// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package processors

import (
	"context"
	"sync"

	"base/counter"
)

type Pipeline struct {
	mu         sync.Mutex
	ctx        context.Context
	index      int64
	processors []IProcessor
}

func NewPipeline(ctx context.Context) *Pipeline {
	return &Pipeline{ctx: ctx}
}

func (pipeline *Pipeline) Metrics() []counter.Metric {
	var metrics []counter.Metric
	for _, proc := range pipeline.processors {
		metrics = append(metrics, proc.Metric())
	}
	return metrics
}

func (pipeline *Pipeline) Add(proc IProcessor) *Pipeline {
	ctx := pipeline.ctx

	pipeline.mu.Lock()
	defer pipeline.mu.Unlock()

	proc.SetContext(ctx)
	if len(pipeline.processors) > 0 {
		source := pipeline.processors[pipeline.index-1]
		proc.From(source)
	}

	pipeline.index++
	pipeline.processors = append(pipeline.processors, proc)
	return pipeline
}

func (pipeline *Pipeline) Run() {
	pipeline.mu.Lock()
	defer pipeline.mu.Unlock()

	for _, proc := range pipeline.processors {
		go func(p IProcessor) {
			p.Execute()
		}(proc)
	}
}

func (pipeline *Pipeline) Pause() {
	pipeline.mu.Lock()
	defer pipeline.mu.Unlock()

	for _, proc := range pipeline.processors {
		go func(p IProcessor) {
			p.Pause()
		}(proc)
	}
}

func (pipeline *Pipeline) Resume() {
	pipeline.mu.Lock()
	defer pipeline.mu.Unlock()

	for _, proc := range pipeline.processors {
		go func(p IProcessor) {
			p.Resume()
		}(proc)
	}
}

func (pipeline *Pipeline) Wait(f func(v interface{}) error) error {
	out := pipeline.Out()
	for x := range out {
		switch x := x.(type) {
		case error:
			return x
		default:
			if err := f(x); err != nil {
				return err
			}
		}
	}
	return nil
}

func (pipeline *Pipeline) Out() <-chan interface{} {
	return pipeline.processors[pipeline.index-1].In().Recv()
}

func (pipeline *Pipeline) Last() IProcessor {
	return pipeline.processors[pipeline.index-1]
}

func (pipeline *Pipeline) String() string {
	pipeline.mu.Lock()
	defer pipeline.mu.Unlock()

	res := ""
	for _, proc := range pipeline.processors {
		res += proc.Name()
		res += " -> "
	}
	return res
}
