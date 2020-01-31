// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package system

import (
	"config"
	"datablocks"

	"base/xlog"
)

type (
	TablesFillFunc    func(*datablocks.DataBlock) error
	DatabasesFillFunc func(*datablocks.DataBlock) error
)
type SystemStorageContext struct {
	log               *xlog.Log
	conf              *config.Config
	tablesFillFunc    TablesFillFunc
	databasesFillFunc DatabasesFillFunc
}

func NewSystemStorageContext(log *xlog.Log, conf *config.Config, tablesFillFunc TablesFillFunc, databasesFillFunc DatabasesFillFunc) *SystemStorageContext {
	return &SystemStorageContext{
		log:               log,
		conf:              conf,
		tablesFillFunc:    tablesFillFunc,
		databasesFillFunc: databasesFillFunc,
	}
}
