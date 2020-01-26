// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package executors

import (
	"planners"
	"processors"
)

type SinkExecutor struct {
	ctx  *ExecutorContext
	plan *planners.SinkPlan
}

func NewSinkExecutor(ctx *ExecutorContext, plan *planners.SinkPlan) *SinkExecutor {
	return &SinkExecutor{
		ctx:  ctx,
		plan: plan,
	}
}

func (executor *SinkExecutor) Name() string {
	return "SinkExecutor"
}

func (executor *SinkExecutor) Execute() (processors.IProcessor, error) {
	return processors.NewSink("transforms_sink"), nil
}

func (executor *SinkExecutor) String() string {
	res := "\n"
	res += "->"
	res += executor.Name()
	res += "\t--> "
	return res
}
