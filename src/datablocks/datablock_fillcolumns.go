// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package datablocks

import (
	"columns"
	"datatypes"
	"expressions"
	"planners"
)

func (block *DataBlock) FillColumnsByPlan(plan *planners.MapPlan) (*DataBlock, error) {
	projects := plan

	// Build the project exprs.
	exprs := make([]expressions.IExpression, projects.Length())
	for i, plan := range projects.SubPlans {
		expr, err := planners.BuildExpressions(plan)
		if err != nil {
			return nil, err
		}
		exprs[i] = expr
	}

	// Get all base fields.
	fields, err := expressions.VariableValues(exprs...)
	if err != nil {
		return nil, err
	}

	columnmap := make(map[string]struct{})
	for i := range block.values {
		columnmap[block.values[i].column.Name] = struct{}{}
	}

	rows := block.NumRows()
	if rows == 0 {
		// If empty, returns header only.
		cols := make([]*columns.Column, len(exprs))
		for i, expr := range exprs {
			cols[i] = columns.NewColumn(expr.String(), datatypes.NewStringDataType())
		}
		return NewDataBlock(cols), nil
	} else {
		params := make(expressions.Map)
		columnValues := make([]*DataBlockValue, 0, 8)

		// Copy the colums from old.
		columnValues = append(columnValues, block.values...)
		for _, expr := range exprs {
			var columnValue *DataBlockValue
			name := expr.String()

			// Check exists.
			if _, ok := columnmap[name]; ok {
				continue
			}

			// Compute the column.
			k := 0
			seqs := block.seqs
			it, err := block.MixsIterator(fields)
			if err != nil {
				return nil, err
			}
			for it.Next() {
				row := it.Value()
				for j := range row {
					params[it.Column(j).Name] = row[j]
				}
				val, err := expr.Eval(params)
				if err != nil {
					return nil, err
				}
				if k == 0 {
					// Get the column type via the expression value.
					dtype, err := datatypes.GetDataTypeByValue(val)
					if err != nil {
						return nil, err
					}
					columnValue = NewDataBlockValueWithCapacity(columns.NewColumn(name, dtype), rows)
				}
				if seqs != nil {
					columnValue.values[seqs[k].AsInt()] = val
				} else {
					columnValue.values[k] = val
				}
				k++
			}
			columnValues = append(columnValues, columnValue)
		}
		return &DataBlock{
			seqs:   block.seqs,
			values: columnValues,
		}, nil
	}
}
