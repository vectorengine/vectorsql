// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package columns

import (
	"datatypes"
)

type ColumnValue struct {
	column Column
	values []datatypes.Value
}

func NewColumnValue(col Column) *ColumnValue {
	return &ColumnValue{
		column: col,
		values: make([]datatypes.Value, 0, 1024),
	}
}

func (cv *ColumnValue) Column() Column {
	return cv.column
}

func (cv *ColumnValue) NumRows() int {
	return len(cv.values)
}

func (cv *ColumnValue) Values() []datatypes.Value {
	return cv.values
}

func (cv *ColumnValue) Insert(v datatypes.Value) error {
	cv.values = append(cv.values, v)
	return nil
}
