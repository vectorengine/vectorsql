// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package transforms

import (
	"datablocks"
	"github.com/gammazero/workerpool"
	"planners"
	"processors"
	"sync/atomic"
)

type FilterTransform struct {
	ctx         *TransformContext
	filter      *planners.FilterPlan
	processRows int64
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
			atomic.AddInt64(&t.processRows, int64(y.NumRows()))
			workerPool.Submit(func() {
				if err := y.FilterByPlan(fields, plan); err != nil {
					out.Send(err)
				} else {
					if y.NumRows() > 0 {
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

func (t *FilterTransform) Rows() int64 {
	return atomic.LoadInt64(&t.processRows)
}
