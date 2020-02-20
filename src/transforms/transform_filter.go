// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package transforms

import (
	"datablocks"
	"planners"
	"processors"
)

type FilterTransform struct {
	ctx    *TransformContext
	filter *planners.FilterPlan
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
	out := t.Out()

	defer out.Close()
	onNext := func(x interface{}) {
		switch y := x.(type) {
		case *datablocks.DataBlock:
			if err := y.FilterByPlan(t.filter); err != nil {
				out.Send(err)
			} else {
				if y.NumRows() > 0 {
					out.Send(y)
				}
			}
		default:
			out.Send(x)
		}
	}
	t.Subscribe(onNext)
}
