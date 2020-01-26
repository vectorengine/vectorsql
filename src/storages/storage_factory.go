// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package storages

import (
	"strings"

	"columns"

	"base/errors"
)

type storageCreator func(*StorageContext, []columns.Column) IStorage

var (
	table = map[string]storageCreator{
		MemoryStorageEngineName:          NewMemoryStorage,
		SystemDatabasesStorageEngineName: NewSystemDatabasesStorage,
		SystemTablesStorageEngineName:    NewSystemTablesStorage,
	}
)

func StorageFactory(ctx *StorageContext, engine string, columns []columns.Column) (IStorage, error) {
	name := strings.ToUpper(engine)
	creator, ok := table[name]
	if !ok {
		return nil, errors.Errorf("Couldn't get the storage:%s", name)
	}
	return creator(ctx, columns), nil
}
