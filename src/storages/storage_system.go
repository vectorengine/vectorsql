// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package storages

import (
	"columns"

	"storages/system"
)

const (
	SystemDatabasesStorageEngineName = "SYSTEM_DATABASES"
	SystemTablesStorageEngineName    = "SYSTEM_TABLES"
	SystemNumbersStorageEngineName   = "SYSTEM_NUMBERS"
)

func NewSystemDatabasesStorage(ctx *StorageContext, cols []columns.Column) IStorage {
	systemCtx := system.NewSystemStorageContext(ctx.log, ctx.conf, ctx.tablesFillFunc, ctx.databasesFillFunc)
	return system.NewSystemDatabasesStorage(systemCtx)
}

func NewSystemTablesStorage(ctx *StorageContext, cols []columns.Column) IStorage {
	systemCtx := system.NewSystemStorageContext(ctx.log, ctx.conf, ctx.tablesFillFunc, ctx.databasesFillFunc)
	return system.NewSystemTablesStorage(systemCtx)
}

func NewSystemNumbersStorage(ctx *StorageContext, cols []columns.Column) IStorage {
	systemCtx := system.NewSystemStorageContext(ctx.log, ctx.conf, ctx.tablesFillFunc, ctx.databasesFillFunc)
	return system.NewSystemNumbersStorage(systemCtx)
}
