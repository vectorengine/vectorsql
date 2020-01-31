// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package databases

import (
	"config"
	"storages"

	"parsers/sqlparser"
)

type Table struct {
	conf    *config.Config
	schema  string
	table   string
	engine  string
	node    *sqlparser.DDL
	storage storages.IStorage
}

func NewTable(conf *config.Config,
	schema string,
	table string,
	engine string,
	node *sqlparser.DDL,
	storage storages.IStorage) *Table {
	return &Table{
		conf:    conf,
		schema:  schema,
		table:   table,
		engine:  engine,
		node:    node,
		storage: storage,
	}
}

func (table *Table) getTable() string {
	return table.table
}

func (table *Table) getDatabase() string {
	return table.schema
}

func (table *Table) getEngine() string {
	return table.engine
}
