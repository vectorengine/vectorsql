// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package executors

import (
	"planners"
	"processors"
	"transforms"
)

type OrderByExecutor struct {
	ctx  *ExecutorContext
	plan *planners.OrderByPlan
}

func NewOrderByExecutor(ctx *ExecutorContext, plan *planners.OrderByPlan) *OrderByExecutor {
	return &OrderByExecutor{
		ctx:  ctx,
		plan: plan,
	}
}

func (executor *OrderByExecutor) Name() string {
	return "OrderByExecutor"
}

func (executor *OrderByExecutor) Execute() (processors.IProcessor, error) {
	log := executor.ctx.log
	conf := executor.ctx.conf

	log.Debug("Executor->Enter->LogicalPlan:%s", executor.plan)
	transformCtx := transforms.NewTransformContext(log, conf)
	transform := transforms.NewOrderByTransform(transformCtx, executor.plan)
	log.Debug("Executor->Return->Pipeline:%v", transform)
	return transform, nil
}

func (executor *OrderByExecutor) String() string {
	res := "\n"
	res += "->"
	res += executor.Name()
	res += "\t--> "
	res += "("
	res += executor.plan.String()
	res += ")"
	return res
}
