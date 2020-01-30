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
		var r interface{}

		switch x := x.(type) {
		case *datablocks.DataBlock:
			y, err := t.orderby(x)
			if err != nil {
				r = err
			} else {
				r = y
			}
		case error:
			r = x
		}
		out.Send(r)
	}
	t.Subscribe(onNext)
}

func (t *OrderByTransform) orderby(x *datablocks.DataBlock) (*datablocks.DataBlock, error) {
	log := t.ctx.log
	plan := t.plan

	for _, order := range plan.Orders {
		expr := order.Expression.(*planners.VariablePlan)
		field := string(expr.Value)
		direction := order.Direction
		log.Debug("transforms->orderby-> field:%v, direction:%v", field, direction)
	}
	return x, nil
}
