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

	log.Debug("Executor->Enter->LogicalPlan:%s", executor.plan)
	transformCtx := transforms.NewTransformContext(executor.ctx.ctx, log, conf)
	transform := transforms.NewOrderByTransform(transformCtx, executor.plan)
	executor.transformer = transform

	result := NewResult()
	result.SetInput(transform)
	log.Debug("Executor->Return->Result:%+v", result)
	return result, nil
}

func (executor *OrderByExecutor) String() string {
	transformer := executor.transformer.(*transforms.OrderByTransform)
	return fmt.Sprintf("(%v, rows:%v, cost:%v)", transformer.Name(), transformer.Rows(), transformer.Duration())
}
