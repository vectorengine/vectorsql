// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package executors

import (
	"reflect"

	"planners"

	"base/errors"
)

type executorCreator func(ctx *ExecutorContext, plan planners.IPlan) IExecutor

var table = map[reflect.Type]executorCreator{
	(reflect.TypeOf(&planners.UsePlan{})):          NewUseExecutor,
	reflect.TypeOf(&planners.SelectPlan{}):         NewSelectExecutor,
	reflect.TypeOf(&planners.CreateDatabasePlan{}): NewCreateDatabaseExecutor,
	reflect.TypeOf(&planners.DropDatabasePlan{}):   NewDropDatabaseExecutor,
	reflect.TypeOf(&planners.CreateTablePlan{}):    NewCreateTableExecutor,
	reflect.TypeOf(&planners.DropTablePlan{}):      NewDropTableExecutor,
	reflect.TypeOf(&planners.ShowDatabasesPlan{}):  NewShowDatabasesExecutor,
	reflect.TypeOf(&planners.ShowTablesPlan{}):     NewShowTablesExecutor,
}

func ExecutorFactory(ctx *ExecutorContext, plan planners.IPlan) (IExecutor, error) {
	creator, ok := table[reflect.TypeOf(plan)]
	if !ok {
		return nil, errors.Errorf("Couldn't get the executor:%T", plan)
	}
	return creator(ctx, plan), nil
}
