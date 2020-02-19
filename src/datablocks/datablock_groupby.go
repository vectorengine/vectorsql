// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package datablocks

import (
	"datavalues"
	"expressions"
	"planners"
)

func (block *DataBlock) GroupByPlan(groupby *planners.GroupByPlan) ([]*DataBlock, error) {
	groupbys := groupby.GroupBys
	projects := groupby.Projects
	hasAggregate := groupby.HasAggregate

	// GroupBy all.
	if groupbys.Length() == 0 {
		group, err := block.FillColumnsByPlan(hasAggregate, projects)
		if err != nil {
			return nil, err
		}
		return []*DataBlock{group}, nil
	} else {
		params := make(expressions.Map)
		hashmap := datavalues.NewHashMap()

		groupbyExprs, err := planners.BuildExpressions(groupbys)
		if err != nil {
			return nil, err
		}

		// Build the groups.
		it := block.RowIterator()
		for it.Next() {
			row := it.Value()
			for i := range row {
				params[it.Column(i).Name] = row[i]
			}

			groupbyValues := make([]*datavalues.Value, len(groupbyExprs))
			for i, expr := range groupbyExprs {
				val, err := expr.Eval(params)
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
			group, err := v.(*DataBlock).FillColumnsByPlan(hasAggregate, groupby.Projects)
			if err != nil {
				return nil, err
			}
			groups[i] = group
			i++
		}
		return groups, nil
	}
}
