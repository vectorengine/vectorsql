// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package datablocks

import (
	"sync"

	"expressions"
	"planners"

	"github.com/gammazero/workerpool"
)

func (block *DataBlock) AggregateSelectionByPlan(fields []string, plan *planners.SelectionPlan) ([]expressions.IExpression, error) {
	var errs []error
	var mu sync.Mutex
	projects := plan.Projects

	projectExprs, err := planners.BuildExpressions(projects)
	if err != nil {
		return nil, err
	}

	workerPool := workerpool.New(len(projectExprs))
	for i := range projectExprs {
		expr := projectExprs[i]
		workerPool.Submit(func() {
			// Compute the column.
			it, err := block.MixsIterator(fields)
			if err != nil {
				errs = append(errs, err)
				return
			}

			params := make(expressions.Map)
			for it.Next() {
				mixed := it.Value()
				for j := range mixed {
					params[it.Column(j).Name] = mixed[j]
				}
				if _, err := expr.Update(params); err != nil {
					mu.Lock()
					errs = append(errs, err)
					mu.Unlock()
					return
				}
			}
		})
	}
	workerPool.StopWait()

	if len(errs) > 0 {
		return nil, errs[0]
	}
	return projectExprs, nil
}
