// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package transforms

import (
	"columns"
	"datatypes"
	"datavalues"
	"expressions"
	"sync"

	"datablocks"
	"planners"
	"processors"
)

type GroupByTransform struct {
	ctx  *TransformContext
	plan *planners.GroupByPlan
	processors.BaseProcessor
}

func NewGroupByTransform(ctx *TransformContext, plan *planners.GroupByPlan) processors.IProcessor {
	return &GroupByTransform{
		ctx:           ctx,
		plan:          plan,
		BaseProcessor: processors.NewBaseProcessor("transform_groupby"),
	}
}

func (t *GroupByTransform) Execute() {
	var wg sync.WaitGroup
	var errors []error
	var blocks []*datablocks.DataBlock

	out := t.Out()
	defer out.Close()

	onNext := func(x interface{}) {
		switch y := x.(type) {
		case *datablocks.DataBlock:
			blocks = append(blocks, y)
		case error:
			errors = append(errors, y)
		}
	}
	onDone := func() {
		defer wg.Done()
		if len(errors) > 0 {
			out.Send(errors[0])
		} else {
			block, err := datablocks.Append(blocks...)
			if err != nil {
				out.Send(err)
			} else {
				if blocks, err := t.groupby(block); err != nil {
					out.Send(err)
				} else {
					for _, x := range blocks {
						out.Send(x)
					}
				}
			}
		}
	}
	wg.Add(1)
	t.Subscribe(onNext, onDone)
	wg.Wait()
}

func (t *GroupByTransform) groupby(x *datablocks.DataBlock) ([]*datablocks.DataBlock, error) {
	var fields []string
	var blocks []*datablocks.DataBlock
	projects := t.plan.Projects

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
			switch val.GetType() {
			case datavalues.TypeInt:
				cols[i] = columns.Column{
					Name:     expr.String(),
					DataType: datatypes.NewInt32DataType(),
				}
			case datavalues.TypeFloat:
				cols[i] = columns.Column{
					Name:     expr.String(),
					DataType: datatypes.NewInt32DataType(),
				}
			case datavalues.TypeString:
				cols[i] = columns.Column{
					Name:     expr.String(),
					DataType: datatypes.NewStringDataType(),
				}
			}
		}
	} else {
		for i, expr := range exprs {
			column, err := x.Column(expr.String())
			if err != nil {
				return nil, err
			}
			cols[i] = column
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
	blocks = append(blocks, newblock)
	return blocks, nil
}
