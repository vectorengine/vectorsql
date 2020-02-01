// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package executors

import (
	"planners"
	"processors"

	"base/errors"
)

type SelectExecutor struct {
	ctx  *ExecutorContext
	plan *planners.SelectPlan
	tree *ExecutorTree
}

func NewSelectExecutor(ctx *ExecutorContext, plan planners.IPlan) IExecutor {
	return &SelectExecutor{
		ctx:  ctx,
		tree: NewExecutorTree(ctx),
		plan: plan.(*planners.SelectPlan),
	}
}

func (executor *SelectExecutor) Execute() (processors.IProcessor, error) {
	ectx := executor.ctx
	log := executor.ctx.log
	tree := executor.tree

	children := executor.plan.SubPlan.SubPlans
	log.Debug("Executor->Enter->LogicalPlan:%s", children)

	for _, plan := range children {
		switch plan := plan.(type) {
		case *planners.TableValuedFunctionPlan:
			executor := NewTableValuedFunctionExecutor(ectx, plan)
			tree.Add(executor)
		case *planners.ProjectPlan:
		case *planners.ScanPlan:
			executor := NewScanExecutor(ectx, plan)
			tree.Add(executor)
		case *planners.FilterPlan:
			executor := NewFilterExecutor(ectx, plan)
			tree.Add(executor)
		case *planners.OrderByPlan:
			executor := NewOrderByExecutor(ectx, plan)
			tree.Add(executor)
		case *planners.SinkPlan:
			executor := NewSinkExecutor(ectx, plan)
			tree.Add(executor)
		default:
			return nil, errors.Errorf("Unsupport plan:%T", plan)
		}
	}
	pipeline, err := tree.BuildPipeline()
	if err != nil {
		return nil, err
	}
	pipeline.Run()

	log.Debug("Executor->Return->Pipeline:%s", pipeline)
	return pipeline.Last(), nil
}

func (executor *SelectExecutor) String() string {
	return "SelectExecutor"
}
