// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package transforms

import (
	"datablocks"
	"planners"
	"processors"
	"sync"
)

type AggregateSelectionTransform struct {
	ctx  *TransformContext
	plan *planners.SelectionPlan
	processors.BaseProcessor
}

func NewAggregateSelectionTransform(ctx *TransformContext, plan *planners.SelectionPlan) processors.IProcessor {
	return &AggregateSelectionTransform{
		ctx:           ctx,
		plan:          plan,
		BaseProcessor: processors.NewBaseProcessor("transform_aggregate_selection"),
	}
}

func (t *AggregateSelectionTransform) Execute() {
	var wg sync.WaitGroup
	var block *datablocks.DataBlock

	out := t.Out()
	defer out.Close()
	plan := t.plan.Projects

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
		defer wg.Done()
		if block != nil {
			if filler, err := block.AggregateSelectionByPlan(plan); err != nil {
				out.Send(err)
			} else {
				out.Send(filler)
			}
		}
	}
	wg.Add(1)
	t.Subscribe(onNext, onDone)
	wg.Wait()
}
