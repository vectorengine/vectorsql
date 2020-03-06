// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package executors

import (
	"databases"
	"planners"
)

type InsertExecutor struct {
	ctx  *ExecutorContext
	plan *planners.InsertPlan
}

func NewInsertExecutor(ctx *ExecutorContext, plan planners.IPlan) IExecutor {
	return &InsertExecutor{
		ctx:  ctx,
		plan: plan.(*planners.InsertPlan),
	}
}

func (executor *InsertExecutor) Execute() (*Result, error) {
	log := executor.ctx.log
	plan := executor.plan
	conf := executor.ctx.conf
	session := executor.ctx.session

	log.Debug("Executor->Enter->LogicalPlan:%s", executor.plan)

	schema := session.GetDatabase()
	if plan.Schema != "" {
		schema = plan.Schema
	}
	table := plan.Table

	databaseCtx := databases.NewDatabaseContext(log, conf)
	storage, err := databases.GetStorage(databaseCtx, schema, table)
	if err != nil {
		return nil, err
	}

	output, err := storage.GetOutputStream(session)
	if err != nil {
		return nil, err
	}

	result := NewResult()
	result.SetOutput(output)
	log.Debug("Executor->Return->Result:%+v", result)
	return result, nil
}

func (executor *InsertExecutor) String() string {
	return ""
}
