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

type OrderByTransform struct {
	ctx  *TransformContext
	plan *planners.OrderByPlan
	processors.BaseProcessor
}

func NewOrderByTransform(ctx *TransformContext, plan *planners.OrderByPlan) processors.IProcessor {
	return &OrderByTransform{
		ctx:           ctx,
		plan:          plan,
		BaseProcessor: processors.NewBaseProcessor("transform_orderby"),
	}
}

func (t *OrderByTransform) Execute() {
	var wg sync.WaitGroup
	var block *datablocks.DataBlock

	out := t.Out()
	defer out.Close()

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
			if err := block.OrderByPlan(t.plan); err != nil {
				out.Send(err)
			} else {
				out.Send(block)
			}
		}
	}
	wg.Add(1)
	t.Subscribe(onNext, onDone)
	wg.Wait()
}
