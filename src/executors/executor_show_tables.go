// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package executors

import (
	"planners"
)

func NewShowTablesExecutor(ctx *ExecutorContext, plan planners.IPlan) IExecutor {
	planner := plan.(*planners.ShowTablesPlan)
	selectPlan := planner.SubPlan.(*planners.SelectPlan)

	mapPlan := planners.NewMapPlan()
	//scan plan
	mapPlan.Add(planners.NewScanPlan("tables", "system"))
	//filter plan
	mapPlan.Add(
		planners.NewFilterPlan(planners.NewBooleanExpressionPlan(
			"=",
			planners.NewVariablePlan("database"),
			planners.NewConstantPlan(ctx.session.GetDatabase()),
		)),
	)
	//orderBy plan
	mapPlan.Add(planners.NewOrderByPlan(
		planners.Order{
			Expression: planners.NewVariablePlan("name"),
			Direction:  "asc",
		},
	))
	//sinker plan
	mapPlan.Add(planners.NewSinkPlan())
	selectPlan.SubPlan = mapPlan
	return NewSelectExecutor(ctx, selectPlan)
}
