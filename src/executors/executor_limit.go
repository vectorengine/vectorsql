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

func NewLimitExecutor(ctx *ExecutorContext, plan *planners.LimitPlan) IExecutor {
	return &LimitExecutor{
		ctx:  ctx,
		plan: plan,
	}
}

func (executor *LimitExecutor) Execute() (*Result, error) {
	log := executor.ctx.log
	conf := executor.ctx.conf

	transformCtx := transforms.NewTransformContext(executor.ctx.ctx, log, conf)
	transform := transforms.NewLimitransform(transformCtx, executor.plan)
	executor.transformer = transform

	result := NewResult()
	result.SetInput(transform)
	return result, nil
}

func (executor *LimitExecutor) String() string {
	transformer := executor.transformer.(*transforms.Limitransform)
	return fmt.Sprintf("(%v, rows:%v, cost:%v)", transformer.Name(), transformer.Rows(), transformer.Duration())
}
