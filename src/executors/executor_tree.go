// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package executors

import (
	"processors"
)

type ExecutorTree struct {
	ctx          *ExecutorContext
	subExecutors []IExecutor
}

func NewExecutorTree(ctx *ExecutorContext) *ExecutorTree {
	return &ExecutorTree{
		ctx: ctx,
	}
}

func (tree *ExecutorTree) Add(executor IExecutor) {
	tree.subExecutors = append(tree.subExecutors, executor)
}

func (tree *ExecutorTree) BuildPipeline() (*processors.Pipeline, error) {
	ectx := tree.ctx

	pipeline := processors.NewPipeline(ectx.ctx)
	for _, executor := range tree.subExecutors {
		transform, err := executor.Execute()
		if err != nil {
			return nil, err
		}
		pipeline.Add(transform)
	}
	return pipeline, nil
}
