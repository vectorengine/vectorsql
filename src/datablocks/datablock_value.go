// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package datablocks

import (
	"columns"
	"datavalues"
)

type ColumnIndex struct {
	Name  string
	Index int
}

type DataBlockValue struct {
	column *columns.Column
	values []datavalues.IDataValue
}

func NewDataBlockValue(col *columns.Column) *DataBlockValue {
	return &DataBlockValue{
		column: col,
		values: make([]datavalues.IDataValue, 0),
	}
}

func NewDataBlockValueWithCapacity(col *columns.Column, capacity int) *DataBlockValue {
	return &DataBlockValue{
		column: col,
		values: make([]datavalues.IDataValue, capacity),
	}
}

func (v *DataBlockValue) ColumnName() string {
	return v.column.Name
}
