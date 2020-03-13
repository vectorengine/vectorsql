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

type FilterExecutor struct {
	ctx         *ExecutorContext
	filter      *planners.FilterPlan
	transformer processors.IProcessor
}

func NewFilterExecutor(ctx *ExecutorContext, filter *planners.FilterPlan) IExecutor {
	return &FilterExecutor{
		ctx:    ctx,
		filter: filter,
	}
}

func (executor *FilterExecutor) Execute() (*Result, error) {
	log := executor.ctx.log
	conf := executor.ctx.conf

	transformCtx := transforms.NewTransformContext(executor.ctx.ctx, log, conf)
	transform := transforms.NewFilterTransform(transformCtx, executor.filter)
	executor.transformer = transform

	result := NewResult()
	result.SetInput(transform)
	return result, nil
}

func (executor *FilterExecutor) String() string {
	transformer := executor.transformer.(*transforms.FilterTransform)
	return fmt.Sprintf("(%v, stats:%+v)", transformer.Name(), transformer.Stats())
}
