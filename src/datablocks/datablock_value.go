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

func newDataBlockValueWithValues(col *columns.Column, values []datavalues.IDataValue) *DataBlockValue {
	return &DataBlockValue{
		column: col,
		values: values,
	}
}

func (v *DataBlockValue) ColumnName() string {
	return v.column.Name
}

func (v *DataBlockValue) DeepClone() *DataBlockValue {
	clone := &DataBlockValue{
		column: v.column,
		values: make([]datavalues.IDataValue, len(v.values)),
	}
	copy(clone.values, v.values)
	return clone
}
