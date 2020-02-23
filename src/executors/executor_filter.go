// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package executors

import (
	"fmt"

	"planners"
	"processors"
	"transforms"
)

type FilterExecutor struct {
	ctx         *ExecutorContext
	filter      *planners.FilterPlan
	transformer processors.IProcessor
}

func NewFilterExecutor(ctx *ExecutorContext, filter *planners.FilterPlan) *FilterExecutor {
	return &FilterExecutor{
		ctx:    ctx,
		filter: filter,
	}
}

func (executor *FilterExecutor) Execute() (processors.IProcessor, error) {
	log := executor.ctx.log
	conf := executor.ctx.conf

	log.Debug("Executor->Enter->LogicalPlan:%s", executor.filter)
	transformCtx := transforms.NewTransformContext(executor.ctx.ctx, log, conf)
	transform := transforms.NewFilterTransform(transformCtx, executor.filter)
	executor.transformer = transform
	log.Debug("Executor->Return->Pipeline:%v", executor.transformer)
	return transform, nil
}

func (executor *FilterExecutor) String() string {
	return fmt.Sprintf("(%v, cost:%v)", executor.transformer.Name(), executor.transformer.Duration())
}
