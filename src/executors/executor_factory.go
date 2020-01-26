// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package executors

import (
	"planners"

	"base/errors"
)

type executorCreator func(ctx *ExecutorContext, plan planners.IPlan) IExecutor

var table = map[string]executorCreator{
	(&planners.UsePlan{}).Name():            NewUseExecutor,
	(&planners.SelectPlan{}).Name():         NewSelectExecutor,
	(&planners.CreateDatabasePlan{}).Name(): NewCreateDatabaseExecutor,
	(&planners.DropDatabasePlan{}).Name():   NewDropDatabaseExecutor,
	(&planners.CreateTablePlan{}).Name():    NewCreateTableExecutor,
	(&planners.DropTablePlan{}).Name():      NewDropTableExecutor,
	(&planners.ShowDatabasesPlan{}).Name():  NewShowDatabasesExecutor,
	(&planners.ShowTablesPlan{}).Name():     NewShowTablesExecutor,
}

func ExecutorFactory(ctx *ExecutorContext, plan planners.IPlan) (IExecutor, error) {
	creator, ok := table[plan.Name()]
	if !ok {
		return nil, errors.Errorf("Couldn't get the executor:%T", plan)
	}
	return creator(ctx, plan), nil
}
