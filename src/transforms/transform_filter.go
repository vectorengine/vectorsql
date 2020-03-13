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

type FilterTransform struct {
	ctx            *TransformContext
	filter         *planners.FilterPlan
	progressValues sessions.ProgressValues
	processors.BaseProcessor
}

func NewFilterTransform(ctx *TransformContext, filter *planners.FilterPlan) processors.IProcessor {
	return &FilterTransform{
		ctx:           ctx,
		filter:        filter,
		BaseProcessor: processors.NewBaseProcessor("transform_filter"),
	}
}

func (t *FilterTransform) Execute() {
	ctx := t.ctx
	plan := t.filter
	out := t.Out()
	defer out.Close()

	// Get all the base fields used by the expression.
	fields, err := planners.BuildVariableValues(plan.SubPlan)
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
				if err := y.FilterByPlan(fields, plan); err != nil {
					out.Send(err)
				} else {
					if y.NumRows() > 0 {
						cost := time.Since(start)
						t.progressValues.Cost.Add(cost)
						t.progressValues.ReadBytes.Add(int64(y.TotalBytes()))
						t.progressValues.ReadRows.Add(int64(y.NumRows()))
						t.progressValues.TotalRowsToRead.Add(int64(y.NumRows()))
						out.Send(y)
					}
				}
			})
		default:
			out.Send(x)
		}
	}
	onDone := func() {
		workerPool.StopWait()
	}
	t.Subscribe(onNext, onDone)
}

func (t *FilterTransform) Stats() sessions.ProgressValues {
	return t.progressValues
}
