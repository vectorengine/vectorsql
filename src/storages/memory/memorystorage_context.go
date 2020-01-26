// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package storages

import (
	"config"

	"base/xlog"
)

type MemoryStorageContext struct {
	log  *xlog.Log
	conf *config.Config
}

func NewMemoryStorageContext(log *xlog.Log, conf *config.Config) *MemoryStorageContext {
	return &MemoryStorageContext{
		log:  log,
		conf: conf,
	}
}
