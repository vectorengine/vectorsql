// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package transforms

import (
	"datablocks"
	"planners"
	"processors"
	"sync/atomic"
)

type ProjectionTransform struct {
	ctx         *TransformContext
	plan        *planners.ProjectionPlan
	processRows int64
	processors.BaseProcessor
}

func NewProjectionTransform(ctx *TransformContext, plan *planners.ProjectionPlan) processors.IProcessor {
	return &ProjectionTransform{
		ctx:           ctx,
		plan:          plan,
		BaseProcessor: processors.NewBaseProcessor("transform_projection"),
	}
}

func (t *ProjectionTransform) Execute() {
	out := t.Out()

	defer out.Close()
	onNext := func(x interface{}) {
		switch y := x.(type) {
		case *datablocks.DataBlock:
			if block, err := y.ProjectionByPlan(t.plan.Projections); err != nil {
				out.Send(err)
			} else {
				out.Send(block)
				atomic.AddInt64(&t.processRows, int64(block.NumRows()))
			}
		default:
			out.Send(x)
		}
	}
	t.Subscribe(onNext)
}

func (t *ProjectionTransform) Rows() int64 {
	return atomic.LoadInt64(&t.processRows)
}
