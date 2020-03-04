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

type ProjectionExecutor struct {
	ctx         *ExecutorContext
	plan        *planners.ProjectionPlan
	transformer processors.IProcessor
}

func NewProjectionExecutor(ctx *ExecutorContext, plan *planners.ProjectionPlan) IExecutor {
	return &ProjectionExecutor{
		ctx:  ctx,
		plan: plan,
	}
}

func (executor *ProjectionExecutor) Execute() (*Result, error) {
	log := executor.ctx.log
	conf := executor.ctx.conf

	log.Debug("Executor->Enter->LogicalPlan:%s", executor.plan)
	transformCtx := transforms.NewTransformContext(executor.ctx.ctx, log, conf)
	transform := transforms.NewProjectionTransform(transformCtx, executor.plan)
	blockIO := NewResult(transform, nil)
	log.Debug("Executor->Return->Result:%+v", blockIO)
	return blockIO, nil
}

func (executor *ProjectionExecutor) String() string {
	transformer := executor.transformer.(*transforms.ProjectionTransform)
	return fmt.Sprintf("(%v, rows:%v, cost:%v)", transformer.Name(), transformer.Rows(), transformer.Duration())
}
