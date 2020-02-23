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

func NewProjectionExecutor(ctx *ExecutorContext, plan *planners.ProjectionPlan) *ProjectionExecutor {
	return &ProjectionExecutor{
		ctx:  ctx,
		plan: plan,
	}
}

func (executor *ProjectionExecutor) Execute() (processors.IProcessor, error) {
	log := executor.ctx.log
	conf := executor.ctx.conf

	log.Debug("Executor->Enter->LogicalPlan:%s", executor.plan)
	transformCtx := transforms.NewTransformContext(executor.ctx.ctx, log, conf)
	transform := transforms.NewProjectionTransform(transformCtx, executor.plan)
	executor.transformer = transform
	log.Debug("Executor->Return->Pipeline:%v", transform)
	return transform, nil
}

func (executor *ProjectionExecutor) String() string {
	return fmt.Sprintf("(%v, cost:%v)", executor.transformer.Name(), executor.transformer.Duration())
}
