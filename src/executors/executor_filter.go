// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package executors

import (
	"planners"
	"processors"
	"transforms"
)

type FilterExecutor struct {
	ctx  *ExecutorContext
	plan *planners.FilterPlan
}

func NewFilterExecutor(ctx *ExecutorContext, plan *planners.FilterPlan) *FilterExecutor {
	return &FilterExecutor{
		ctx:  ctx,
		plan: plan,
	}
}

func (executor *FilterExecutor) Execute() (processors.IProcessor, error) {
	log := executor.ctx.log
	conf := executor.ctx.conf

	log.Debug("Executor->Enter->LogicalPlan:%s", executor.plan)
	transformCtx := transforms.NewTransformContext(log, conf)
	transform := transforms.NewFilterTransform(transformCtx, executor.plan)
	log.Debug("Executor->Return->Pipeline:%v", transform)
	return transform, nil
}

func (executor *FilterExecutor) String() string {
	return "FilterExecutor"
}
