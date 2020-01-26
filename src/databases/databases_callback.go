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
	columns := block.ColumnValues()
	for _, database := range databases.databases {
		if err := columns[0].Insert(datatypes.MakeString(database.Meta().GetDBName())); err != nil {
			return err
		}
		if err := columns[1].Insert(datatypes.MakeString(database.Meta().GetEngineName())); err != nil {
			return err
		}
		if err := columns[2].Insert(datatypes.MakeString(database.Meta().GetDataPath())); err != nil {
			return err
		}
		if err := columns[3].Insert(datatypes.MakeString(database.Meta().GetMetaDataPath())); err != nil {
			return err
		}
	}
	return nil
}

func fillTablesFunc(dbName string, block *datablocks.DataBlock) error {
	database, err := databases.getDatabase(dbName)
	if err != nil {
		return err
	}

	columns := block.ColumnValues()
	tables := database.GetTables()
	for _, table := range tables {
		if err := columns[0].Insert(datatypes.MakeString(table.getTable())); err != nil {
			return err
		}
		if err := columns[1].Insert(datatypes.MakeString(table.getEngine())); err != nil {
			return err
		}
	}
	return nil
}
