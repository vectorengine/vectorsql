// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package executors

import (
	"databases"
	"planners"
)

type UseExecutor struct {
	ctx  *ExecutorContext
	plan *planners.UsePlan
}

func NewUseExecutor(ctx *ExecutorContext, plan planners.IPlan) IExecutor {
	return &UseExecutor{
		ctx:  ctx,
		plan: plan.(*planners.UsePlan),
	}
}

func (executor *UseExecutor) Execute() (*Result, error) {
	ectx := executor.ctx
	plan := executor.plan

	dbname := plan.Ast.DBName.String()
	if _, err := databases.GetDatabase(dbname); err != nil {
		return nil, err
	}
	ectx.session.SetDatabase(dbname)

	result := NewResult()
	return result, nil
}

func (executor *UseExecutor) String() string {
	return ""
}
