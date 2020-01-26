// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package executors

import (
	"planners"
)

func NewShowTablesExecutor(ctx *ExecutorContext, plan planners.IPlan) IExecutor {
	planner := plan.(*planners.ShowTablesPlan)
	return NewSelectExecutor(ctx, planner.SubPlan)
}
