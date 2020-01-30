// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package transforms

import (
	"datablocks"
	"datatypes"
	"functions"
	"planners"
	"processors"

	"base/errors"
)

type FilterTransform struct {
	ctx  *TransformContext
	plan *planners.FilterPlan
	processors.BaseProcessor
}

func NewFilterTransform(ctx *TransformContext, plan *planners.FilterPlan) processors.IProcessor {
	return &FilterTransform{
		ctx:           ctx,
		plan:          plan,
		BaseProcessor: processors.NewBaseProcessor("transform_filter"),
	}
}

func (t *FilterTransform) Execute() {
	out := t.Out()

	defer out.Close()
	onNext := func(x interface{}) {
		switch y := x.(type) {
		case *datablocks.DataBlock:
			if err := t.filter(y); err != nil {
				x = err
			}
		}
		out.Send(x)
	}
	t.Subscribe(onNext)
}

func (t *FilterTransform) filter(x *datablocks.DataBlock) error {
	plan := t.plan.SubPlan

	checks, err := t.check(x, plan)
	if err != nil {
		return err
	}
	return x.Filter(checks)
}

func (t *FilterTransform) check(x *datablocks.DataBlock, plan planners.IPlan) ([]*datatypes.Value, error) {
	switch plan := plan.(type) {
	case *planners.BooleanExpressionPlan:
		checks := make([]*datatypes.Value, x.NumRows())

		right := datatypes.ToValue(plan.Args[1].(*planners.ConstantPlan).Value)

		colName := plan.Args[0].(*planners.VariablePlan).Value
		iter, err := x.Iterator(colName)
		if err != nil {
			return nil, err
		}

		function, err := functions.FunctionFactory(plan.FuncName)
		if err != nil {
			return nil, err
		}

		i := 0
		for iter.Next() {
			left := iter.Value()
			if err := function.Validator.Validate(left, right); err != nil {
				return nil, err
			}
			result, err := function.Logic(left, right)
			if err != nil {
				return nil, err
			}
			checks[i] = result
			i++
		}
		return checks, nil
	case *planners.AndPlan:
		checksLeft, err := t.check(x, plan.Left)
		if err != nil {
			return nil, err
		}
		checksRight, err := t.check(x, plan.Right)
		if err != nil {
			return nil, err
		}

		function, err := functions.FunctionFactory(plan.FuncName)
		if err != nil {
			return nil, err
		}
		for i := range checksLeft {
			r, err := function.Logic(checksLeft[i], checksRight[i])
			if err != nil {
				return nil, err
			}
			checksLeft[i] = r
		}
		return checksLeft, nil
	case *planners.OrPlan:
		checksLeft, err := t.check(x, plan.Left)
		if err != nil {
			return nil, err
		}
		checksRight, err := t.check(x, plan.Right)
		if err != nil {
			return nil, err
		}

		function, err := functions.FunctionFactory(plan.FuncName)
		if err != nil {
			return nil, err
		}
		for i := range checksLeft {
			r, err := function.Logic(checksLeft[i], checksRight[i])
			if err != nil {
				return nil, err
			}
			checksLeft[i] = r
		}
		return checksLeft, nil
	}
	return nil, errors.Errorf("unknow plan:%T", plan)
}
