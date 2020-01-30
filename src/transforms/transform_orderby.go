// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package transforms

import (
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
	out := t.Out()

	defer out.Close()
	onNext := func(x interface{}) {
		switch y := x.(type) {
		case *datablocks.DataBlock:
			if err := t.orderby(y); err != nil {
				x = err
			}
		}
		out.Send(x)
	}
	t.Subscribe(onNext)
}

func (t *OrderByTransform) orderby(x *datablocks.DataBlock) error {
	plan := t.plan

	var sorters []datablocks.Sorter
	for _, order := range plan.Orders {
		expr := order.Expression.(*planners.VariablePlan)
		field := string(expr.Value)
		direction := order.Direction
		sorters = append(sorters, datablocks.NewSorter(field, direction))
	}
	return x.Sort(sorters...)
}
