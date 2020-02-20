// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package transforms

import (
	"datablocks"
	"planners"
	"processors"
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
	out := t.Out()
	defer out.Close()

	onNext := func(x interface{}) {
		switch y := x.(type) {
		case *datablocks.DataBlock:
			if block, err := y.NormalSelectionByPlan(t.plan.Projects); err != nil {
				out.Send(err)
			} else {
				out.Send(block)
			}
		case error:
			out.Send(y)
		}
	}
	t.Subscribe(onNext)
}
