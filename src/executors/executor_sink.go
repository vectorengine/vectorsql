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

func NewSinkExecutor(ctx *ExecutorContext, plan *planners.SinkPlan) IExecutor {
	return &SinkExecutor{
		ctx:  ctx,
		plan: plan,
	}
}

func (executor *SinkExecutor) Execute() (*Result, error) {
	proc := processors.NewSink("transforms_sink")
	return NewResult(proc, nil), nil
}

func (executor *SinkExecutor) String() string {
	return ""
}
