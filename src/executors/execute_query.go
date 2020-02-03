// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package executors

import (
	"context"
	"processors"

	"base/xlog"
	"config"
	"optimizers"
	"planners"
	"sessions"
)

// ExecuteQuery execute the query and returns a sink
func ExecuteQuery(ctx context.Context, query string, log *xlog.Log, conf *config.Config, session *sessions.Session) (sink processors.IProcessor, err error) {
	// Logical plans.
	plan, err := planners.PlanFactory(query)
	if err != nil {
		log.Error("%+v", err)
		return
	}
	plan = optimizers.Optimize(plan, optimizers.DefaultOptimizers)
	// Executors.
	ectx := NewExecutorContext(ctx, log, conf, session)
	executor, err := ExecutorFactory(ectx, plan)
	if err != nil {
		log.Error("%+v", err)
		return
	}

	sink, err = executor.Execute()
	if err != nil {
		log.Error("%+v", err)
		return
	}
	return
}
