// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package transforms

import (
	"time"

	"datablocks"
	"planners"
	"processors"
	"sessions"
)

type OrderByTransform struct {
	ctx            *TransformContext
	plan           *planners.OrderByPlan
	progressValues sessions.ProgressValues
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
			start := time.Now()
			if err := block.OrderByPlan(fields, t.plan); err != nil {
				out.Send(err)
			} else {
				cost := time.Since(start)
				t.progressValues.Cost.Add(cost)
				t.progressValues.ReadBytes.Add(int64(block.TotalBytes()))
				t.progressValues.ReadRows.Add(int64(block.NumRows()))
				t.progressValues.TotalRowsToRead.Add(int64(block.NumRows()))
				out.Send(block)
			}
		}
	}
	t.Subscribe(onNext, onDone)
}

func (t *OrderByTransform) Stats() sessions.ProgressValues {
	return t.progressValues
}
