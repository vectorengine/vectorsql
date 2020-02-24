// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package datablocks

import (
	"datavalues"
	"expressions"
	"planners"
)

func (block *DataBlock) GroupBySelectionByPlan(plan *planners.SelectionPlan) (*datavalues.HashMap, error) {
	projects := plan.Projects
	groupbys := plan.GroupBys

	params := make(expressions.Map)
	hashmap := datavalues.NewHashMap()

	groupbyExprs, err := planners.BuildExpressions(groupbys)
	if err != nil {
		return nil, err
	}

	// Build groups.
	iter := block.RowIterator()
	for iter.Next() {
		row := iter.Value()
		for i := range row {
			params[iter.Column(i).Name] = row[i]
		}

		// GroupBy key.
		groupbyValues := make([]datavalues.IDataValue, len(groupbyExprs))
		for i, expr := range groupbyExprs {
			val, err := expr.Update(params)
			if err != nil {
				return nil, err
			}
			groupbyValues[i] = val
		}
		var groupKey datavalues.IDataValue
		if len(groupbyExprs) > 1 {
			groupKey = datavalues.MakeTuple(groupbyValues...)
		} else {
			groupKey = groupbyValues[0]
		}
		projectExprs, hash, ok, err := hashmap.Get(groupKey)
		if err != nil {
			return nil, err
		}
		if !ok {
			if projectExprs, err = planners.BuildExpressions(projects); err != nil {
				return nil, err
			}
			if err := hashmap.SetByHash(groupKey, hash, projectExprs); err != nil {
				return nil, err
			}
		}

		// Update the project expressions.
		for _, expr := range projectExprs.([]expressions.IExpression) {
			if _, err := expr.Update(params); err != nil {
				return nil, err
			}
		}
	}
	return hashmap, nil
}
