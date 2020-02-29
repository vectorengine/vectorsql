// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package transforms

import (
	"sync"
	"sync/atomic"

	"base/collections"
	"datablocks"
	"expressions"
	"planners"
	"processors"

	"github.com/gammazero/workerpool"
)

type GroupBySelectionTransform struct {
	ctx         *TransformContext
	plan        *planners.SelectionPlan
	processRows int64
	processors.BaseProcessor
}

func NewGroupBySelectionTransform(ctx *TransformContext, plan *planners.SelectionPlan) processors.IProcessor {
	return &GroupBySelectionTransform{
		ctx:           ctx,
		plan:          plan,
		BaseProcessor: processors.NewBaseProcessor("transform_groupby_selection"),
	}
}

func (t *GroupBySelectionTransform) Execute() {
	ctx := t.ctx
	plan := t.plan
	out := t.Out()
	defer out.Close()

	var mu sync.Mutex
	groupers := make([]*collections.HashMap, 0, 32)
	workerPool := workerpool.New(ctx.conf.Runtime.ParallelWorkerNumber)

	onNext := func(x interface{}) {
		switch y := x.(type) {
		case *datablocks.DataBlock:
			workerPool.Submit(func() {
				grouper, err := y.GroupBySelectionByPlan(plan)
				if err != nil {
					out.Send(err)
					return
				}
				mu.Lock()
				groupers = append(groupers, grouper)
				mu.Unlock()
				atomic.AddInt64(&t.processRows, int64(y.NumRows()))
			})
		case error:
			out.Send(y)
		}
	}

	onDone := func() {
		workerPool.StopWait()
		final := collections.NewHashMap()
		for _, grouper := range groupers {
			iter := grouper.GetIterator()
			for {
				curKey, curVal, ok := iter.Next()
				if !ok {
					break
				}

				// Check.
				mergeVal, mergeHash, ok, err := final.Get(curKey)
				if err != nil {
					out.Send(err)
					return
				}

				// Merge state.
				if ok {
					curVal := curVal.([]expressions.IExpression)
					mergeVal := mergeVal.([]expressions.IExpression)
					for i := range mergeVal {
						if _, err := mergeVal[i].Merge(curVal[i]); err != nil {
							out.Send(err)
							return
						}
					}

				} else {
					if err := final.SetByHash(curKey, mergeHash, curVal); err != nil {
						out.Send(err)
						return
					}
				}
			}
		}

		// Final state.
		iter := final.GetIterator()
		for {
			_, val, ok := iter.Next()
			if !ok {
				break
			}
			if finalBlock, err := datablocks.BuildOneBlockFromExpressions(val.([]expressions.IExpression)); err != nil {
				out.Send(err)
			} else {
				out.Send(finalBlock)
			}
		}
	}
	t.Subscribe(onNext, onDone)
}

func (t *GroupBySelectionTransform) Rows() int64 {
	return atomic.LoadInt64(&t.processRows)
}
