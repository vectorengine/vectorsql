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

type OrderByExecutor struct {
	ctx         *ExecutorContext
	plan        *planners.OrderByPlan
	transformer processors.IProcessor
}

func NewOrderByExecutor(ctx *ExecutorContext, plan *planners.OrderByPlan) IExecutor {
	return &OrderByExecutor{
		ctx:  ctx,
		plan: plan,
	}
}

func (executor *OrderByExecutor) Execute() (*Result, error) {
	log := executor.ctx.log
	conf := executor.ctx.conf

	transformCtx := transforms.NewTransformContext(executor.ctx.ctx, log, conf)
	transform := transforms.NewOrderByTransform(transformCtx, executor.plan)
	executor.transformer = transform

	result := NewResult()
	result.SetInput(transform)
	return result, nil
}

func (executor *OrderByExecutor) String() string {
	transformer := executor.transformer.(*transforms.OrderByTransform)
	return fmt.Sprintf("(%v, stats:%+v)", transformer.Name(), transformer.Stats())
}
