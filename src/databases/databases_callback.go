// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package databases

import (
	"datablocks"
	"datavalues"
)

// Handlers.
func fillDatabasesFunc(block *datablocks.DataBlock) error {
	for _, database := range databases.databases {
		if err := block.WriteRow([]*datavalues.Value{
			datavalues.MakeString(database.Meta().GetDBName()),
			datavalues.MakeString(database.Meta().GetEngineName()),
			datavalues.MakeString(database.Meta().GetDataPath()),
			datavalues.MakeString(database.Meta().GetMetaDataPath()),
		},
		); err != nil {
			return err
		}
	}
	return nil
}

func fillTablesFunc(block *datablocks.DataBlock) error {
	for _, database := range databases.databases {
		tables := database.GetTables()
		for _, table := range tables {
			if err := block.WriteRow([]*datavalues.Value{
				datavalues.MakeString(table.getTable()),
				datavalues.MakeString(table.getDatabase()),
				datavalues.MakeString(table.getEngine()),
			},
			); err != nil {
				return err
			}
		}
	}
	return nil
}
