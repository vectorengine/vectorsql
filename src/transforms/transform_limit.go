// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package transforms

import (
	"time"

	"datablocks"
	"planners"
	"processors"
	"sessions"
)

type Limitransform struct {
	ctx            *TransformContext
	plan           *planners.LimitPlan
	progressValues sessions.ProgressValues
	processors.BaseProcessor
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
		limit  int
		offset int
	)

	//Todo support eval(variable)
	offset = t.plan.OffsetPlan.(*planners.ConstantPlan).Value.(int)
	limit = t.plan.RowcountPlan.(*planners.ConstantPlan).Value.(int)

	out := t.Out()
	defer out.Close()

	onNext := func(x interface{}) {
		switch y := x.(type) {
		case *datablocks.DataBlock:
			if x != nil {
				start := time.Now()
				cutOffset, cutLimit := y.Limit(offset, limit)
				offset -= cutOffset
				limit -= cutLimit
				x = y

				cost := time.Since(start)
				t.progressValues.Cost.Add(cost)
				t.progressValues.ReadBytes.Add(int64(y.TotalBytes()))
				t.progressValues.ReadRows.Add(int64(y.NumRows()))
				t.progressValues.TotalRowsToRead.Add(int64(y.NumRows()))
			}
		}
		out.Send(x)

		if offset < 0 || limit <= 0 {
			out.Close()
			return
		}
	}
	t.Subscribe(onNext)
}

func (t *Limitransform) Stats() sessions.ProgressValues {
	return t.progressValues
}
