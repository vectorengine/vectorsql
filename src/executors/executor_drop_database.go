// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package executors

import (
	"databases"
	"planners"
	"processors"
)

type DropDatabaseExecutor struct {
	ctx  *ExecutorContext
	plan *planners.DropDatabasePlan
}

func NewDropDatabaseExecutor(ctx *ExecutorContext, plan planners.IPlan) IExecutor {
	return &DropDatabaseExecutor{
		ctx:  ctx,
		plan: plan.(*planners.DropDatabasePlan),
	}
}

func (executor *DropDatabaseExecutor) Execute() (processors.IProcessor, error) {
	ectx := executor.ctx
	log := executor.ctx.log
	ast := executor.plan.Ast

	log.Debug("Executor->Enter->LogicalPlan:%s", executor.plan)
	databaseCtx := databases.NewDatabaseContext(ectx.log, ectx.conf)
	database, err := databases.DatabaseFactory(databaseCtx, ast)
	if err != nil {
		return nil, err
	}
	if err := database.Executor().DropDatabase(); err != nil {
		return nil, err
	}
	log.Debug("Executor->Return->Pipeline:%v", nil)
	return nil, nil
}
