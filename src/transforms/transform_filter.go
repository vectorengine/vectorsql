// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package transforms

import (
	"datablocks"
	"datavalues"
	"expressions"
	"planners"
	"processors"
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

func (t *FilterTransform) check(x *datablocks.DataBlock, plan planners.IPlan) ([]*datavalues.Value, error) {
	type field struct {
		name string
		idx  int
	}
	var fields []field

	expr, err := planners.BuildExpressions(plan)
	if err != nil {
		return nil, err
	}
	if err := plan.Walk(func(p planners.IPlan) (bool, error) {
		switch p := p.(type) {
		case *planners.VariablePlan:
			name := string(p.Value)
			idx, err := x.ColumnIndex(name)
			if err != nil {
				return false, err
			}
			fields = append(fields, field{name: name, idx: idx})
		}
		return true, nil
	}); err != nil {
		return nil, err
	}

	i := 0
	checks := make([]*datavalues.Value, x.NumRows())
	params := make(expressions.Map, len(fields))
	rowiter := x.RowIterator()
	for rowiter.Next() {
		row := rowiter.Value()
		for _, field := range fields {
			params[field.name] = row[field.idx]
		}
		v, err := expr.Eval(params)
		if err != nil {
			return nil, err
		}
		checks[i] = v
		i++
	}
	return checks, nil
}
