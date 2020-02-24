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

func (block *DataBlock) NormalSelectionByPlan(fields []string, plan *planners.MapPlan) (*DataBlock, error) {
	projects := plan

	projectExprs, err := planners.BuildExpressions(projects)
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
		cols := make([]*columns.Column, len(projectExprs))
		for i, expr := range projectExprs {
			cols[i] = columns.NewColumn(expr.String(), datatypes.NewStringDataType())
		}
		return NewDataBlock(cols), nil
	} else {
		params := make(expressions.Map)
		columnValues := make([]*DataBlockValue, 0, 8)

		// Copy the colums from old.
		columnValues = append(columnValues, block.values...)
		for _, expr := range projectExprs {
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
				val, err := expr.Update(params)
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
					columnValue.values[seqs[k]] = val
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
