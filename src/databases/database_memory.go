// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package databases

import (
	"storages"

	"parsers/sqlparser"
)

const (
	MemoryDatabaseName = "MEMORY"
)

type MemoryDatabase struct {
	onDiskDatabase IDatabase
}

func NewMemoryDatabase(ctx *DatabaseContext, node *sqlparser.DBDDL) IDatabase {
	return &MemoryDatabase{
		onDiskDatabase: NewOnDiskDatabase(ctx, node),
	}
}

func (database *MemoryDatabase) Load() error {
	return nil
}

func (database *MemoryDatabase) Name() string {
	return MemoryDatabaseName
}

func (database *MemoryDatabase) Executor() *ExecuteFuns {
	onDiskDatabase := database.onDiskDatabase
	return onDiskDatabase.Executor()
}

func (database *MemoryDatabase) Meta() *MetaFuns {
	onDiskDatabase := database.onDiskDatabase
	return onDiskDatabase.Meta()
}

func (database *MemoryDatabase) GetTables() []*Table {
	onDiskDatabase := database.onDiskDatabase
	return onDiskDatabase.GetTables()
}

func (database *MemoryDatabase) GetStorage(tablename string) (storages.IStorage, error) {
	onDiskDatabase := database.onDiskDatabase
	return onDiskDatabase.GetStorage(tablename)
}

func registerMemoryDatabase(factory *databaseFactory) {
	factory.register(MemoryDatabaseName, NewMemoryDatabase)
}
