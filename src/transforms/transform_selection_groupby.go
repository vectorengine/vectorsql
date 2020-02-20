// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package transforms

import (
	"sync"

	"datablocks"
	"planners"
	"processors"
)

type GroupBySelectionTransform struct {
	ctx  *TransformContext
	plan *planners.SelectionPlan
	processors.BaseProcessor
}

func NewGroupBySelectionTransform(ctx *TransformContext, plan *planners.SelectionPlan) processors.IProcessor {
	return &GroupBySelectionTransform{
		ctx:           ctx,
		plan:          plan,
		BaseProcessor: processors.NewBaseProcessor("transform_groupby_selection"),
	}
}

func (t *GroupBySelectionTransform) Execute() {
	var wg sync.WaitGroup
	var block *datablocks.DataBlock

	out := t.Out()
	defer out.Close()
	plan := t.plan

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
			if filler, err := block.GroupBySelectionByPlan(plan); err != nil {
				out.Send(err)
			} else {
				for _, blk := range filler {
					out.Send(blk)
				}
			}
		}
	}
	wg.Add(1)
	t.Subscribe(onNext, onDone)
	wg.Wait()
}
