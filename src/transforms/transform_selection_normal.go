// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package transforms

import (
	"datablocks"
	"planners"
	"processors"
	"sessions"
	"time"

	"github.com/gammazero/workerpool"
)

type NormalSelectionTransform struct {
	ctx            *TransformContext
	plan           *planners.SelectionPlan
	progressValues sessions.ProgressValues
	processors.BaseProcessor
}

func NewNormalSelectionTransform(ctx *TransformContext, plan *planners.SelectionPlan) processors.IProcessor {
	return &NormalSelectionTransform{
		ctx:           ctx,
		plan:          plan,
		BaseProcessor: processors.NewBaseProcessor("transform_normal_selection"),
	}
}

func (t *NormalSelectionTransform) Execute() {
	ctx := t.ctx
	out := t.Out()
	defer out.Close()
	plan := t.plan

	// Get all base fields by the expression.
	fields, err := planners.BuildVariableValues(plan.Projects)
	if err != nil {
		out.Send(err)
		return
	}

	workerPool := workerpool.New(ctx.conf.Runtime.ParallelWorkerNumber)
	onNext := func(x interface{}) {
		switch y := x.(type) {
		case *datablocks.DataBlock:
			workerPool.Submit(func() {
				start := time.Now()
				if block, err := y.NormalSelectionByPlan(fields, plan); err != nil {
					out.Send(err)
				} else {
					cost := time.Since(start)
					t.progressValues.Cost.Add(cost)
					t.progressValues.ReadBytes.Add(int64(y.TotalBytes()))
					t.progressValues.ReadRows.Add(int64(y.NumRows()))
					t.progressValues.TotalRowsToRead.Add(int64(y.NumRows()))
					out.Send(block)
				}
			})
		case error:
			out.Send(y)
		}
	}
	onDone := func() {
		workerPool.StopWait()
	}
	t.Subscribe(onNext, onDone)
}

func (t *NormalSelectionTransform) Stats() sessions.ProgressValues {
	return t.progressValues
}
