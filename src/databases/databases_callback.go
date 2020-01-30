// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package databases

import (
	"sort"

	"datablocks"
	"datatypes"
)

// Handlers.
func fillDatabasesFunc(block *datablocks.DataBlock) error {
	var dbs []IDatabase

	for _, database := range databases.databases {
		dbs = append(dbs, database)
	}
	sort.Slice(dbs, func(i, j int) bool { return dbs[i].Name() < dbs[j].Name() })

	batcher := datablocks.NewBatchWriter(block.Columns())
	for _, database := range dbs {
		if err := batcher.WriteRow(
			datatypes.MakeString(database.Meta().GetDBName()),
			datatypes.MakeString(database.Meta().GetEngineName()),
			datatypes.MakeString(database.Meta().GetDataPath()),
			datatypes.MakeString(database.Meta().GetMetaDataPath()),
		); err != nil {
			return err
		}
	}
	return block.Write(batcher)
}

func fillTablesFunc(dbName string, block *datablocks.DataBlock) error {
	database, err := databases.getDatabase(dbName)
	if err != nil {
		return err
	}

	batcher := datablocks.NewBatchWriter(block.Columns())
	tables := database.GetTables()
	for _, table := range tables {
		if err := batcher.WriteRow(
			datatypes.MakeString(table.getTable()),
			datatypes.MakeString(table.getEngine()),
		); err != nil {
			return err
		}
	}
	return block.Write(batcher)
}
