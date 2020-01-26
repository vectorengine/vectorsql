// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package databases

import (
	"storages"

	"parsers/sqlparser"
)

const (
	OrdinaryDatabaseName = "ORDINARY"
)

type OrdinaryDatabase struct {
	onDiskDatabase IDatabase
}

func NewOrdinaryDatabase(ctx *DatabaseContext, node *sqlparser.DBDDL) IDatabase {
	return &OrdinaryDatabase{
		onDiskDatabase: NewOnDiskDatabase(ctx, node),
	}
}

func (database *OrdinaryDatabase) Load() error {
	onDiskDatabase := database.onDiskDatabase
	return onDiskDatabase.Load()
}

func (database *OrdinaryDatabase) Name() string {
	return OrdinaryDatabaseName
}

func (database *OrdinaryDatabase) Executor() *ExecuteFuns {
	onDiskDatabase := database.onDiskDatabase
	return onDiskDatabase.Executor()
}

func (database *OrdinaryDatabase) Meta() *MetaFuns {
	onDiskDatabase := database.onDiskDatabase
	return onDiskDatabase.Meta()
}

func (database *OrdinaryDatabase) GetTables() []*Table {
	onDiskDatabase := database.onDiskDatabase
	return onDiskDatabase.GetTables()
}

func (database *OrdinaryDatabase) GetStorage(tablename string) (storages.IStorage, error) {
	onDiskDatabase := database.onDiskDatabase
	return onDiskDatabase.GetStorage(tablename)
}

func registerOrdinaryDatabase(factory *databaseFactory) {
	factory.register(OrdinaryDatabaseName, NewOrdinaryDatabase)
}
