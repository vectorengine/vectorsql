// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package executors

import (
	"databases"
	"planners"
)

type CreateTableExecutor struct {
	ctx  *ExecutorContext
	plan *planners.CreateTablePlan
}

func NewCreateTableExecutor(ctx *ExecutorContext, plan planners.IPlan) IExecutor {
	return &CreateTableExecutor{
		ctx:  ctx,
		plan: plan.(*planners.CreateTablePlan),
	}
}

func (executor *CreateTableExecutor) Execute() (*Result, error) {
	ectx := executor.ctx
	log := executor.ctx.log
	ast := executor.plan.Ast

	log.Debug("Executor->Enter->LogicalPlan:%s", executor.plan)
	schema := ectx.session.GetDatabase()
	if !ast.Table.Qualifier.IsEmpty() {
		schema = ast.Table.Qualifier.String()
	}

	database, err := databases.GetDatabase(schema)
	if err != nil {
		return nil, err
	}
	if err := database.Executor().CreateTable(ast); err != nil {
		return nil, err
	}

	result := NewResult()
	log.Debug("Executor->Return->Result:%+v", result)
	return result, nil
}

func (executor *CreateTableExecutor) String() string {
	return ""
}
