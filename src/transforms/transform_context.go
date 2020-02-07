// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package transforms

import (
	"base/xlog"
	"config"
	"context"
)

type TransformContext struct {
	ctx  context.Context
	log  *xlog.Log
	conf *config.Config
}

func NewTransformContext(ctx context.Context, log *xlog.Log, conf *config.Config) *TransformContext {
	return &TransformContext{
		ctx:  ctx,
		log:  log,
		conf: conf,
	}
}
