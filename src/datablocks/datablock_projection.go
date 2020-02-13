// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package datablocks

import (
	"columns"
	"datatypes"
	"datavalues"
	"expressions"
	"planners"
)

// ProjectByPlan --
// Creates a new data block with the less slice moving.
// If the column have been exists, just copy it to the new block.
// if the column not exists, compute it with the expression.
func (block *DataBlock) ProjectByPlan(plan *planners.ProjectionPlan) (*DataBlock, error) {
	projects := plan.Projections

	// Build the project exprs.
	exprs := make([]expressions.IExpression, projects.Length())
	for i, plan := range projects.SubPlans {
		expr, err := planners.BuildExpressions(plan)
		if err != nil {
			return nil, err
		}
		exprs[i] = expr
	}

	rows := block.NumRows()
	if rows == 0 {
		// If empty, returns header only.
		cols := make([]columns.Column, len(exprs))
		for i, expr := range exprs {
			cols[i] = columns.Column{Name: expr.String(), DataType: datatypes.NewStringDataType()}
		}
		return NewDataBlock(cols), nil
	} else {
		params := make(expressions.Map)
		values := make([]*DataBlockValue, len(exprs))
		for i, expr := range exprs {
			ok := false
			name := expr.String()
			for _, val := range block.values {
				if val.column.Name == name {
					ok = true
					values[i] = val
					break
				}
			}

			if !ok {
				// Get the column type.
				{
					it := block.RowIterator()
					it.Next()
					row := it.Value()
					for k := range row {
						params[it.Column(k).Name] = row[k]
					}
					val, err := expr.Eval(params)
					if err != nil {
						return nil, err
					}
					dtype, err := datatypes.GetDataTypeByValue(val)
					if err != nil {
						return nil, err
					}
					values[i] = NewDataBlockValueWithCapacity(columns.NewColumn(name, dtype), rows)
				}

				// Get the values.
				{
					k := 0
					seqs := block.seqs
					values[i].values = make([]*datavalues.Value, rows)
					it := block.RowIterator()
					for it.Next() {
						row := it.Value()
						for k := range row {
							params[it.Column(k).Name] = row[k]
						}
						val, err := expr.Eval(params)
						if err != nil {
							return nil, err
						}
						if seqs != nil {
							values[i].values[seqs[k].AsInt()] = val
						} else {
							values[i].values[k] = val
						}
						k++
					}
				}
			}
		}
		return &DataBlock{
			seqs:      block.seqs,
			values:    values,
			immutable: true,
		}, nil
	}
}
