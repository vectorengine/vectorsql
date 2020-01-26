// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package databases

import (
	"storages"

	"parsers/sqlparser"
)

type MetaFuns struct {
	GetDBName       func() string
	GetDataPath     func() string
	GetEngineName   func() string
	GetMetaDataPath func() string
}

type ExecuteFuns struct {
	CreateDatabase func() error
	DropDatabase   func() error
	CreateTable    func(*sqlparser.DDL) error
	DropTable      func(string) error
}

type IDatabase interface {
	Name() string
	Load() error
	Meta() *MetaFuns
	Executor() *ExecuteFuns
	GetTables() []*Table
	GetStorage(string) (storages.IStorage, error)
}
