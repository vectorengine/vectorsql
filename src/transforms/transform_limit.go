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

type Limitransform struct {
	ctx  *TransformContext
	plan *planners.LimitPlan
	processors.BaseProcessor

	current int
}

func NewLimitransform(ctx *TransformContext, plan *planners.LimitPlan) processors.IProcessor {
	return &Limitransform{
		ctx:           ctx,
		plan:          plan,
		BaseProcessor: processors.NewBaseProcessor("transform_limit"),
	}
}

func (t *Limitransform) Execute() {
	var (
		wg     sync.WaitGroup
		limit  int
		offset int
	)

	//Todo support variable
	offset = t.plan.OffsetPlan.(*planners.ConstantPlan).Value.(int)
	limit = t.plan.RowcountPlan.(*planners.ConstantPlan).Value.(int)

	out := t.Out()
	defer out.Close()

	onNext := func(x interface{}) {
		switch y := x.(type) {
		case *datablocks.DataBlock:
			if x != nil {
				if offset < 0 || limit <= 0 {
					x = nil
					break
				}

				z, err := y.Limit(offset, limit)
				if err != nil {
					x = err
					break
				}
				if z.NumRows() == 0 {
					offset -= y.NumRows()
				}
				limit -= y.NumRows()
				x = z
			}
		}
		out.Send(x)
	}
	wg.Add(1)
	t.Subscribe(onNext)
	wg.Wait()
}
