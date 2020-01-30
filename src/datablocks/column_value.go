// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package datablocks

import (
	"columns"
	"datatypes"
)

type ColumnValue struct {
	column columns.Column
	values []*datatypes.Value
}

func NewColumnValue(col columns.Column) *ColumnValue {
	return &ColumnValue{
		column: col,
		values: make([]*datatypes.Value, 0, 1024),
	}
}

func (cv *ColumnValue) NumRows() int {
	return len(cv.values)
}
