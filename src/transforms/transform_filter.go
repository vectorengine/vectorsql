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
			if err := t.check(y); err != nil {
				x = err
			}
		}
		out.Send(x)
	}
	t.Subscribe(onNext)
}

func (t *FilterTransform) check(x *datablocks.DataBlock) error {
	var fields []string
	filterPlan := t.filter

	expr, err := planners.BuildExpressions(filterPlan.SubPlan)
	if err != nil {
		return err
	}

	if err := filterPlan.Walk(func(p planners.IPlan) (bool, error) {
		switch p := p.(type) {
		case *planners.VariablePlan:
			fields = append(fields, string(p.Value))
		}
		return true, nil
	}); err != nil {
		return err
	}
	colidxs, err := x.ColumnIndexes(fields...)
	if err != nil {
		return err
	}

	i := 0
	checks := make([]*datavalues.Value, x.NumRows())
	params := make(expressions.Map, len(fields))
	rowiter := x.RowIterator()
	for rowiter.Next() {
		row := rowiter.Value()
		for _, colidx := range colidxs {
			params[colidx.Name] = row[colidx.Index]
		}
		v, err := expr.Eval(params)
		if err != nil {
			return err
		}
		checks[i] = v
		i++
	}
	return x.Filter(checks)
}
