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

func (block *DataBlock) NormalSelectionByPlan(fields []string, plan *planners.SelectionPlan) (*DataBlock, error) {
	projects := plan.Projects

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

			values := make([]datavalues.IDataValue, rows)
			for it.Next() {
				row := it.Value()
				for j := range row {
					params[it.Column(j).Name] = row[j]
				}
				val, err := expr.Update(params)
				if err != nil {
					return nil, err
				}
				if seqs != nil {
					values[seqs[k]] = val
				} else {
					values[k] = val
				}
				k++
			}

			// Get the column type via the expression value.
			dtype, err := datatypes.GetDataTypeByValue(expr.Result())
			if err != nil {
				return nil, err
			}
			columnValue := newDataBlockValueWithValues(columns.NewColumn(name, dtype), values)
			columnValues = append(columnValues, columnValue)
		}
		return &DataBlock{
			seqs:   block.seqs,
			values: columnValues,
		}, nil
	}
}
