// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package executors

import (
	"fmt"

	"base/errors"
	"planners"
	"processors"
	"transforms"
)

type SelectionExecutor struct {
	ctx         *ExecutorContext
	plan        *planners.SelectionPlan
	transformer processors.IProcessor
}

func NewSelectionExecutor(ctx *ExecutorContext, plan *planners.SelectionPlan) IExecutor {
	return &SelectionExecutor{
		ctx:  ctx,
		plan: plan,
	}
}

func (executor *SelectionExecutor) Execute() (*Result, error) {
	log := executor.ctx.log
	conf := executor.ctx.conf
	plan := executor.plan

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
	executor.transformer = transform

	result := NewResult()
	result.SetInput(transform)
	return result, nil
}

func (executor *SelectionExecutor) String() string {
	transformer := executor.transformer
	switch transformer := transformer.(type) {
	case *transforms.NormalSelectionTransform:
		return fmt.Sprintf("(%v, stats:%+v)", transformer.Name(), transformer.Stats())
	case *transforms.AggregateSelectionTransform:
		return fmt.Sprintf("(%v, stats:%+v)", transformer.Name(), transformer.Stats())
	case *transforms.GroupBySelectionTransform:
		return fmt.Sprintf("(%v, stats:%+v)", transformer.Name(), transformer.Stats())
	}
	return ""
}
