// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package executors

import (
	"context"

	"base/xlog"
	"config"
	"sessions"
)

type ExecutorContext struct {
	log              *xlog.Log
	ctx              context.Context
	conf             *config.Config
	session          *sessions.Session
	progressCallback func(values *sessions.ProgressValues)
}

func NewExecutorContext(ctx context.Context, log *xlog.Log, conf *config.Config, session *sessions.Session) *ExecutorContext {
	return &ExecutorContext{
		log:     log,
		ctx:     ctx,
		conf:    conf,
		session: session,
	}
}

func (ctx *ExecutorContext) SetProgressCallback(fn func(pv *sessions.ProgressValues)) {
	ctx.progressCallback = fn
}
