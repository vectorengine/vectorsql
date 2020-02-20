// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package executors

import (
	"base/errors"
	"planners"
	"processors"
	"transforms"
)

type SelectionExecutor struct {
	ctx  *ExecutorContext
	plan *planners.SelectionPlan
}

func NewSelectionExecutor(ctx *ExecutorContext, plan *planners.SelectionPlan) *SelectionExecutor {
	return &SelectionExecutor{
		ctx:  ctx,
		plan: plan,
	}
}

func (executor *SelectionExecutor) Execute() (processors.IProcessor, error) {
	log := executor.ctx.log
	conf := executor.ctx.conf
	plan := executor.plan

	log.Debug("Executor->Enter->LogicalPlan:%s", executor.plan)
	transformCtx := transforms.NewTransformContext(executor.ctx.ctx, log, conf)

	var transform processors.IProcessor
	switch plan.SelectionMode {
	case planners.NormalSelection:
		transform = transforms.NewNormalSelectionTransform(transformCtx, executor.plan)
	case planners.AggregateSelection:
		transform = transforms.NewAggregateSelectionTransform(transformCtx, executor.plan)
	case planners.GroupBySelection:
		transform = transforms.NewGroupBySelectionTransform(transformCtx, executor.plan)
	default:
		return nil, errors.Errorf("Unsupported filler mode:%v", plan.SelectionMode)
	}
	log.Debug("Executor->Return->Pipeline:%v", transform)
	return transform, nil
}
