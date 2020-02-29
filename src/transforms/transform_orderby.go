// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package transforms

import (
	"sync/atomic"

	"datablocks"
	"planners"
	"processors"
)

type OrderByTransform struct {
	ctx         *TransformContext
	plan        *planners.OrderByPlan
	processRows int64
	processors.BaseProcessor
}

func NewOrderByTransform(ctx *TransformContext, plan *planners.OrderByPlan) processors.IProcessor {
	return &OrderByTransform{
		ctx:           ctx,
		plan:          plan,
		BaseProcessor: processors.NewBaseProcessor("transform_orderby"),
	}
}

func (t *OrderByTransform) Execute() {
	var block *datablocks.DataBlock

	plan := t.plan
	out := t.Out()
	defer out.Close()

	// Get all base fields by the expression.
	fields, err := planners.BuildVariableValues(plan)
	if err != nil {
		out.Send(err)
		return
	}

	onNext := func(x interface{}) {
		switch y := x.(type) {
		case *datablocks.DataBlock:
			if block == nil {
				block = y
			} else {
				if err := block.Append(y); err != nil {
					out.Send(err)
				}
			}
		case error:
			out.Send(y)
		}
	}
	onDone := func() {
		if block != nil {
			if err := block.OrderByPlan(fields, t.plan); err != nil {
				out.Send(err)
			} else {
				out.Send(block)
				atomic.AddInt64(&t.processRows, int64(block.NumRows()))
			}
		}
	}
	t.Subscribe(onNext, onDone)
}

func (t *OrderByTransform) Rows() int64 {
	return atomic.LoadInt64(&t.processRows)
}
