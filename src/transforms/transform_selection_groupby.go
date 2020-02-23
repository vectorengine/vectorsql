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

type GroupBySelectionTransform struct {
	ctx  *TransformContext
	plan *planners.SelectionPlan
	processors.BaseProcessor
}

func NewGroupBySelectionTransform(ctx *TransformContext, plan *planners.SelectionPlan) processors.IProcessor {
	return &GroupBySelectionTransform{
		ctx:           ctx,
		plan:          plan,
		BaseProcessor: processors.NewBaseProcessor("transform_groupby_selection"),
	}
}

func (t *GroupBySelectionTransform) Execute() {
	plan := t.plan
	out := t.Out()
	defer out.Close()

	params := make(expressions.Map)
	hashmap := datavalues.NewHashMap()

	groupbyExprs, err := planners.BuildExpressions(plan.GroupBys)
	if err != nil {
		out.Send(err)
		return
	}

	mergeFn := func(p expressions.Map) error {
		groupbyValues := make([]datavalues.IDataValue, len(groupbyExprs))
		for i, expr := range groupbyExprs {
			val, err := expr.Update(p)
			if err != nil {
				return err
			}
			groupbyValues[i] = val
		}
		key := datavalues.MakeTuple(groupbyValues...)
		projectExprs, hash, ok, err := hashmap.Get(key)
		if err != nil {
			return err
		}
		if !ok {
			projectExprs, err = planners.BuildExpressions(plan.Projects)
			if err != nil {
				return err
			}
			if err := hashmap.SetByHash(key, hash, projectExprs); err != nil {
				return err
			}
		}

		for _, expr := range projectExprs.([]expressions.IExpression) {
			if _, err := expr.Update(p); err != nil {
				return err
			}
		}
		return nil
	}

	buildFn := func(exprs []expressions.IExpression) (*datablocks.DataBlock, error) {
		row := make([]datavalues.IDataValue, len(exprs))
		column := make([]*columns.Column, len(exprs))
		for i, expr := range exprs {
			if res, err := expr.Result(); err != nil {
				return nil, err
			} else {
				row[i] = res
				// Get the column type via the expression value.
				dtype, err := datatypes.GetDataTypeByValue(res)
				if err != nil {
					return nil, err
				}
				column[i] = columns.NewColumn(expr.String(), dtype)
			}
		}
		group := datablocks.NewDataBlock(column)
		if err := group.WriteRow(row); err != nil {
			return nil, err
		}
		return group, nil
	}

	onNext := func(x interface{}) {
		switch y := x.(type) {
		case *datablocks.DataBlock:
			it := y.RowIterator()
			for it.Next() {
				row := it.Value()
				for i := range row {
					params[it.Column(i).Name] = row[i]
				}
				if err := mergeFn(params); err != nil {
					out.Send(err)
					return
				}
			}
		case error:
			out.Send(y)
		}
	}
	onDone := func() {
		iter := hashmap.GetIterator()
		for {
			v, ok := iter.Next()
			if !ok {
				break
			}
			if group, err := buildFn(v.([]expressions.IExpression)); err != nil {
				out.Send(err)
			} else {
				out.Send(group)
			}
		}
	}
	t.Subscribe(onNext, onDone)
}
