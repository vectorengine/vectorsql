// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package datablocks

import (
	"strings"

	"base/collections"
	"expressions"
	"planners"
)

func (block *DataBlock) GroupBySelectionByPlan(plan *planners.SelectionPlan) (*collections.HashMap, error) {
	projects := plan.Projects
	groupbys := plan.GroupBys

	params := make(expressions.Map)
	hashmap := collections.NewHashMap()

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
		groupbykeys := make([]string, len(groupbyExprs))
		for i, expr := range groupbyExprs {
			val, err := expr.Update(params)
			if err != nil {
				return nil, err
			}
			groupbykeys[i] = val.Show()
		}
		key := strings.Join(groupbykeys, "")
		projectExprs, hash, ok, err := hashmap.Get(key)
		if err != nil {
			return nil, err
		}
		if !ok {
			if projectExprs, err = planners.BuildExpressions(projects); err != nil {
				return nil, err
			}
			if err := hashmap.SetByHash(key, hash, projectExprs); err != nil {
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
