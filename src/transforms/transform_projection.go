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

type ProjectionTransform struct {
	ctx            *TransformContext
	plan           *planners.ProjectionPlan
	progressValues sessions.ProgressValues
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
			start := time.Now()
			if block, err := y.ProjectionByPlan(t.plan.Projections); err != nil {
				out.Send(err)
			} else {
				cost := time.Since(start)
				t.progressValues.Cost.Add(cost)
				t.progressValues.ReadBytes.Add(int64(block.TotalBytes()))
				t.progressValues.ReadRows.Add(int64(block.NumRows()))
				t.progressValues.TotalRowsToRead.Add(int64(block.NumRows()))
				out.Send(block)
			}
		default:
			out.Send(x)
		}
	}
	t.Subscribe(onNext)
}

func (t *ProjectionTransform) Stats() sessions.ProgressValues {
	return t.progressValues
}
