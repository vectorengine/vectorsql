// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package datablocks

import (
	"datavalues"
	"expressions"
	"planners"
)

func (block *DataBlock) GroupBySelectionByPlan(groupby *planners.SelectionPlan) ([]*DataBlock, error) {
	groupbys := groupby.GroupBys
	projects := groupby.Projects

	params := make(expressions.Map)
	hashmap := datavalues.NewHashMap()

	groupbyExprs, err := planners.BuildExpressions(groupbys)
	if err != nil {
		return nil, err
	}

	// Build groups.
	it := block.RowIterator()
	for it.Next() {
		row := it.Value()
		for i := range row {
			params[it.Column(i).Name] = row[i]
		}

		groupbyValues := make([]*datavalues.Value, len(groupbyExprs))
		for i, expr := range groupbyExprs {
			val, err := expr.Update(params)
			if err != nil {
				return nil, err
			}
			groupbyValues[i] = val
		}
		key := datavalues.MakeTuple(groupbyValues...)
		v, hash, ok, err := hashmap.Get(key)
		if err != nil {
			return nil, err
		}
		if !ok {
			v = block.Clone()
			if err := hashmap.SetByHash(key, hash, v); err != nil {
				return nil, err
			}
		}
		// Write.
		if err := v.(*DataBlock).WriteRow(row); err != nil {
			return nil, err
		}
	}

	// Build blocks from group hashmap.
	i := 0
	groups := make([]*DataBlock, hashmap.Count())
	iter := hashmap.GetIterator()
	for {
		v, ok := iter.Next()
		if !ok {
			break
		}
		group, err := v.(*DataBlock).AggregateSelectionByPlan(projects)
		if err != nil {
			return nil, err
		}
		groups[i] = group
		i++
	}
	return groups, nil
}
