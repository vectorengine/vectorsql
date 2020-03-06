// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package storages

import (
	"columns"
	"datastreams"
	"sessions"
)

type IStorage interface {
	Name() string
	Columns() []*columns.Column
	GetInputStream(*sessions.Session) (datastreams.IDataBlockInputStream, error)
	GetOutputStream(*sessions.Session) (datastreams.IDataBlockOutputStream, error)
}
