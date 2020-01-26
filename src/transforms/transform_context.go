// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package transforms

import (
	"config"

	"base/xlog"
)

type TransformContext struct {
	log  *xlog.Log
	conf *config.Config
}

func NewTransformContext(log *xlog.Log, conf *config.Config) *TransformContext {
	return &TransformContext{
		log:  log,
		conf: conf,
	}
}
