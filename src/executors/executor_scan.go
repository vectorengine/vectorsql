// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package executors

import (
	"fmt"

	"databases"
	"planners"
	"processors"
	"transforms"
)

type ScanExecutor struct {
	ctx         *ExecutorContext
	plan        *planners.ScanPlan
	transformer processors.IProcessor
}

func NewScanExecutor(ctx *ExecutorContext, plan *planners.ScanPlan) IExecutor {
	return &ScanExecutor{
		ctx:  ctx,
		plan: plan,
	}
}

func (executor *ScanExecutor) Execute() (*Result, error) {
	log := executor.ctx.log
	conf := executor.ctx.conf
	plan := executor.plan
	session := executor.ctx.session

	log.Debug("Executor->Enter->LogicalPlan:%s", executor.plan)
	if plan.Schema == "" {
		plan.Schema = session.GetDatabase()
	}

	databaseCtx := databases.NewDatabaseContext(log, conf)
	storage, err := databases.GetStorage(databaseCtx, plan.Schema, plan.Table)
	if err != nil {
		return nil, err
	}

	input, err := storage.GetInputStream(session)
	if err != nil {
		return nil, err
	}
	transformCtx := transforms.NewTransformContext(executor.ctx.ctx, log, conf)
	transformCtx.SetProgressCallback(executor.ctx.progressCallback)
	transform := transforms.NewDataSourceTransform(transformCtx, input)
	executor.transformer = transform

	result := NewResult()
	result.SetInput(transform)
	log.Debug("Executor->Return->Result:%+v", result)
	return result, nil
}

func (executor *ScanExecutor) String() string {
	transformer := executor.transformer.(*transforms.DataSourceTransform)
	return fmt.Sprintf("(%v, rows:%v, cost:%v)", transformer.Name(), transformer.Rows(), transformer.Duration())
}
