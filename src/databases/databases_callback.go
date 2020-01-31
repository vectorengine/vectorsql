// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package databases

import (
	"datablocks"
	"datatypes"
)

// Handlers.
func fillDatabasesFunc(block *datablocks.DataBlock) error {
	batcher := datablocks.NewBatchWriter(block.Columns())
	for _, database := range databases.databases {
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

func fillTablesFunc(block *datablocks.DataBlock) error {
	batcher := datablocks.NewBatchWriter(block.Columns())
	for _, database := range databases.databases {
		tables := database.GetTables()
		for _, table := range tables {
			if err := batcher.WriteRow(
				datatypes.MakeString(table.getTable()),
				datatypes.MakeString(table.getDatabase()),
				datatypes.MakeString(table.getEngine()),
			); err != nil {
				return err
			}
		}
	}
	return block.Write(batcher)
}
