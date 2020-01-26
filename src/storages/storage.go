// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package storages

import (
	"columns"
	"datablocks"
	"planners"
	"sessions"
)

type IStorage interface {
	Name() string
	Columns() []columns.Column
	GetInputStream(*sessions.Session, *planners.ScanPlan) (datablocks.IDataBlockInputStream, error)
	GetOutputStream(*sessions.Session, *planners.ScanPlan) (datablocks.IDataBlockOutputStream, error)
}
