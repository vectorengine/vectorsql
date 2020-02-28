// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package transforms

import (
	"datablocks"
	"planners"
	"processors"

	"github.com/gammazero/workerpool"
)

type NormalSelectionTransform struct {
	ctx  *TransformContext
	plan *planners.SelectionPlan
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
				if block, err := y.NormalSelectionByPlan(fields, plan); err != nil {
					out.Send(err)
				} else {
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
