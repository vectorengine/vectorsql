// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package executors

import (
	"databases"
	"planners"
	"processors"
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

func (executor *UseExecutor) Execute() (processors.IProcessor, error) {
	ectx := executor.ctx
	log := executor.ctx.log
	plan := executor.plan

	log.Debug("Executor->Enter->LogicalPlan:%s", executor.plan)
	dbname := plan.Ast.DBName.String()
	if _, err := databases.GetDatabase(dbname); err != nil {
		return nil, err
	}
	ectx.session.SetDatabase(dbname)
	log.Debug("Executor->Return->Pipeline:%v", nil)
	return nil, nil
}
