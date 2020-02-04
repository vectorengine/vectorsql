// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package transforms

import (
	"fmt"
	"sync"

	"columns"
	"datablocks"
	"datavalues"
	"planners"
	"processors"
)

type GroupByTransform struct {
	ctx  *TransformContext
	plan *planners.GroupByPlan
	processors.BaseProcessor
}

func NewGroupByTransform(ctx *TransformContext, plan *planners.GroupByPlan) processors.IProcessor {
	return &GroupByTransform{
		ctx:           ctx,
		plan:          plan,
		BaseProcessor: processors.NewBaseProcessor("transform_groupby"),
	}
}

func (t *GroupByTransform) Execute() {
	var wg sync.WaitGroup
	var errors []error
	var blocks []*datablocks.DataBlock

	out := t.Out()
	defer out.Close()

	onNext := func(x interface{}) {
		switch y := x.(type) {
		case *datablocks.DataBlock:
			blocks = append(blocks, y)
		case error:
			errors = append(errors, y)
		}
	}
	onDone := func() {
		defer wg.Done()
		if len(errors) > 0 {
			out.Send(errors[0])
		} else {
			block, err := datablocks.Append(blocks...)
			if err != nil {
				out.Send(err)
			} else {
				if blocks, err := t.groupby(block); err != nil {
					out.Send(err)
				} else {
					for _, x := range blocks {
						out.Send(x)
					}
				}
			}
		}
	}
	wg.Add(1)
	t.Subscribe(onNext, onDone)
	wg.Wait()
}

func (t *GroupByTransform) groupby(x *datablocks.DataBlock) ([]*datablocks.DataBlock, error) {
	log := t.ctx.log
	projects := t.plan.Projects
	groupbys := t.plan.GroupBys

	type col struct {
		name     string
		aggrFunc string
		column   columns.Column
	}

	plans := projects.AsPlans()
	cols := make([]col, len(plans))
	for i := range plans {
		log.Debug("%+v", plans[i])
		switch plan := plans[i].(type) {
		case *planners.VariablePlan:
			column, err := x.Column(string(plan.Value))
			if err != nil {
				return nil, err
			}
			cols[i] = col{
				name:   column.Name,
				column: column,
			}
		case *planners.FunctionExpressionPlan:
			column, err := x.Column(string(plan.Args[0].(*planners.VariablePlan).Value))
			if err != nil {
				return nil, err
			}

			oldName := column.Name
			newName := fmt.Sprintf("%s(%s)", plan.FuncName, column.Name)
			column.Name = newName
			cols[i] = col{
				name:     oldName,
				column:   column,
				aggrFunc: plan.FuncName,
			}
		}
	}
	log.Debug("Transform->GroupBy->Projects: %+s", projects)

	var newBlockColumns []columns.Column
	for i := range cols {
		newBlockColumns = append(newBlockColumns, cols[i].column)
	}

	var groupbyCols []string
	groupbyPlans := groupbys.AsPlans()
	for i := range groupbyPlans {
		switch plan := groupbyPlans[i].(type) {
		case *planners.VariablePlan:
			groupbyCols = append(groupbyCols, string(plan.Value))
		}
	}
	blocks, err := x.GroupBy(groupbyCols)
	if err != nil {
		return nil, err
	}

	var results []*datablocks.DataBlock
	for i := range blocks {
		var rows []*datavalues.Value
		block := blocks[i]
		newblock := datablocks.NewDataBlock(newBlockColumns)

		for j := range cols {
			val, err := block.Aggregator(cols[j].aggrFunc, cols[j].name)
			if err != nil {
				return nil, err
			}
			rows = append(rows, val)
		}
		if err := newblock.WriteRow(rows); err != nil {
			return nil, err
		}
		results = append(results, newblock)
	}
	return results, nil
}
