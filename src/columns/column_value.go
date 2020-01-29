// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package columns

import (
	"datatypes"
)

type ColumnValue struct {
	Column Column
	Values []datatypes.Value
}

func NewColumnValue(col Column) *ColumnValue {
	return &ColumnValue{
		Column: col,
		Values: make([]datatypes.Value, 0, 1024),
	}
}

func (cv *ColumnValue) NumRows() int {
	return len(cv.Values)
}

func (cv *ColumnValue) Insert(v datatypes.Value) error {
	cv.Values = append(cv.Values, v)
	return nil
}
