// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package transforms

import (
	"columns"
	"datablocks"
	"datatypes"
	"datavalues"
	"expressions"
	"planners"
	"processors"
)

type ProjectionTransform struct {
	ctx  *TransformContext
	plan *planners.ProjectionPlan
	processors.BaseProcessor
}

func NewProjectionTransform(ctx *TransformContext, plan *planners.ProjectionPlan) processors.IProcessor {
	return &ProjectionTransform{
		ctx:           ctx,
		plan:          plan,
		BaseProcessor: processors.NewBaseProcessor("transform_projection"),
	}
}

func (t *ProjectionTransform) Execute() {
	out := t.Out()

	defer out.Close()
	onNext := func(x interface{}) {
		switch y := x.(type) {
		case *datablocks.DataBlock:
			if block, err := t.project(y); err != nil {
				out.Send(err)
			} else {
				out.Send(block)
			}
		default:
			out.Send(x)
		}
	}
	t.Subscribe(onNext)
}

func (t *ProjectionTransform) project(x *datablocks.DataBlock) (*datablocks.DataBlock, error) {
	var fields []string
	projects := t.plan.Projections

	// Build the fields which used by projects.
	if err := projects.Walk(func(p planners.IPlan) (bool, error) {
		switch p := p.(type) {
		case *planners.VariablePlan:
			fields = append(fields, string(p.Value))
		}
		return true, nil
	}); err != nil {
		return nil, err
	}

	// Column index.
	colidxs, err := x.ColumnIndexes(fields...)
	if err != nil {
		return nil, err
	}

	// Build the project exprs.
	exprs := make([]expressions.IExpression, projects.Length())
	for i, plan := range projects.SubPlans {
		expr, err := planners.BuildExpressions(plan)
		if err != nil {
			return nil, err
		}
		exprs[i] = expr
	}

	// Build the projects column.
	cols := make([]columns.Column, len(exprs))
	params := make(expressions.Map, len(colidxs))
	it := x.RowIterator()
	if it.Next() {
		rows := it.Value()
		for _, colidx := range colidxs {
			params[colidx.Name] = rows[colidx.Index]
		}
		for i, expr := range exprs {
			val, err := expr.Eval(params)
			if err != nil {
				return nil, err
			}
			dataType, err := datatypes.GetDataTypeByValue(val)
			if err != nil {
				return nil, err
			}
			cols[i] = columns.Column{
				Name:     expr.String(),
				DataType: dataType,
			}
		}
	} else {
		for i, expr := range exprs {
			cols[i] = columns.Column{Name: expr.String(), DataType: datatypes.NewStringDataType()}
		}
	}

	newblock := datablocks.NewDataBlock(cols)
	it = x.RowIterator()
	for it.Next() {
		row := it.Value()
		for _, colidx := range colidxs {
			params[colidx.Name] = row[colidx.Index]
		}
		newrow := make([]*datavalues.Value, len(exprs))
		for i, expr := range exprs {
			val, err := expr.Eval(params)
			if err != nil {
				return nil, err
			}
			newrow[i] = val
		}
		if err := newblock.WriteRow(newrow); err != nil {
			return nil, err
		}
	}
	return newblock, nil
}
