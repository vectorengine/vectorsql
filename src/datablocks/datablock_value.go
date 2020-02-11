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
	column columns.Column
	values []*datavalues.Value
}

func NewDataBlockValue(col columns.Column) *DataBlockValue {
	return &DataBlockValue{
		column: col,
		values: make([]*datavalues.Value, 0, 1024),
	}
}

func (v *DataBlockValue) NumRows() int {
	return len(v.values)
}

func (v *DataBlockValue) ColumnName() string {
	return v.column.Name
}
