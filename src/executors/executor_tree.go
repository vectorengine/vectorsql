// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package executors

import (
	"fmt"
	"strings"

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

func (tree *ExecutorTree) String() string {
	res := ""
	for _, child := range tree.subExecutors {
		res += fmt.Sprintf("%+v", child)
		//res += child.String()
		res += ", "
	}
	res = strings.TrimRight(res, ", ")
	return res
}
