// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package executors

import (
	"databases"
	"planners"
	"processors"
	"transforms"
)

type ScanExecutor struct {
	ctx  *ExecutorContext
	plan *planners.ScanPlan
}

func NewScanExecutor(ctx *ExecutorContext, plan *planners.ScanPlan) *ScanExecutor {
	return &ScanExecutor{
		ctx:  ctx,
		plan: plan,
	}
}

func (executor *ScanExecutor) Name() string {
	return "ScanExecutor"
}

func (executor *ScanExecutor) Execute() (processors.IProcessor, error) {
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

	input, err := storage.GetInputStream(session, plan)
	if err != nil {
		return nil, err
	}
	transformCtx := transforms.NewTransformContext(log, conf)
	transform := transforms.NewDataSourceTransform(transformCtx, input)
	log.Debug("Executor->Return->Pipeline:%v", transform)
	return transform, nil
}

func (executor *ScanExecutor) String() string {
	res := "\n"
	res += "->"
	res += executor.Name()
	res += "\t--> "
	res += executor.plan.String()
	return res
}
