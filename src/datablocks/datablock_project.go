// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package datablocks

import (
	"columns"
	"datatypes"
	"planners"
)

func (block *DataBlock) ProjectByPlan(plan *planners.MapPlan) (*DataBlock, error) {
	projects := plan

	// Build the project exprs.
	exprs, err := planners.BuildExpressions(projects)
	if err != nil {
		return nil, err
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
		columnValues := make([]*DataBlockValue, len(exprs))
		for i, expr := range exprs {
			columnValue, err := block.DataBlockValue(expr.String())
			if err != nil {
				return nil, err
			}
			columnValues[i] = columnValue
		}
		return newDataBlock(block.seqs, columnValues), nil
	}
}
