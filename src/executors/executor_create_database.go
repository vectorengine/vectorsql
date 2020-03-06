// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package executors

import (
	"databases"
	"planners"
)

type CreateDatabaseExecutor struct {
	ctx  *ExecutorContext
	plan *planners.CreateDatabasePlan
}

func NewCreateDatabaseExecutor(ctx *ExecutorContext, plan planners.IPlan) IExecutor {
	return &CreateDatabaseExecutor{
		ctx:  ctx,
		plan: plan.(*planners.CreateDatabasePlan),
	}
}

func (executor *CreateDatabaseExecutor) Execute() (*Result, error) {
	ectx := executor.ctx
	log := executor.ctx.log
	ast := executor.plan.Ast

	log.Debug("Executor->Enter->LogicalPlan:%s", executor.plan)
	databaseCtx := databases.NewDatabaseContext(ectx.log, ectx.conf)
	database, err := databases.DatabaseFactory(databaseCtx, ast)
	if err != nil {
		return nil, err
	}
	if err := database.Executor().CreateDatabase(); err != nil {
		return nil, err
	}

	result := NewResult()
	log.Debug("Executor->Return->Result:%+v", result)
	return result, nil
}

func (executor *CreateDatabaseExecutor) String() string {
	return ""
}
