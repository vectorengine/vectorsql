// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package datablocks

import (
	"columns"
	"datatypes"
)

type DataBlockIterator struct {
	cv      *ColumnValue
	current int
}

func NewDataBlockIterator(cv *ColumnValue) *DataBlockIterator {
	return &DataBlockIterator{
		cv:      cv,
		current: -1,
	}
}

func (it *DataBlockIterator) Column() columns.Column {
	return it.cv.column
}

func (it *DataBlockIterator) Next() bool {
	it.current++
	return it.current < it.cv.NumRows()
}

func (it *DataBlockIterator) Value() datatypes.Value {
	return it.cv.values[it.current]
}
