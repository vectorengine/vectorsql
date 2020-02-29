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

type LimitExecutor struct {
	ctx         *ExecutorContext
	plan        *planners.LimitPlan
	transformer processors.IProcessor
}

func NewLimitExecutor(ctx *ExecutorContext, plan *planners.LimitPlan) *LimitExecutor {
	return &LimitExecutor{
		ctx:  ctx,
		plan: plan,
	}
}

func (executor *LimitExecutor) Execute() (processors.IProcessor, error) {
	log := executor.ctx.log
	conf := executor.ctx.conf

	log.Debug("Executor->Enter->LogicalPlan:%s", executor.plan)
	transformCtx := transforms.NewTransformContext(executor.ctx.ctx, log, conf)
	transform := transforms.NewLimitransform(transformCtx, executor.plan)
	executor.transformer = transform
	log.Debug("Executor->Return->Pipeline:%v", transform)
	return transform, nil
}

func (executor *LimitExecutor) String() string {
	transformer := executor.transformer.(*transforms.Limitransform)
	return fmt.Sprintf("(%v, rows:%v, cost:%v)", transformer.Name(), transformer.Rows(), transformer.Duration())
}
