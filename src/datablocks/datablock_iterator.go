// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package datablocks

import (
	"columns"
	"datatypes"
)

type DataBlockIterator struct {
	cv      *DataBlockValue
	seqs    []*datatypes.Value
	current int
}

func newDataBlockIterator(seqs []*datatypes.Value, cv *DataBlockValue) *DataBlockIterator {
	return &DataBlockIterator{
		cv:      cv,
		seqs:    seqs,
		current: -1,
	}
}

func (it *DataBlockIterator) Column() columns.Column {
	return it.cv.column
}

func (it *DataBlockIterator) Next() bool {
	rows := it.cv.NumRows()
	if it.seqs != nil {
		rows = len(it.seqs)
	}
	it.current++
	return it.current < rows
}

func (it *DataBlockIterator) Value() *datatypes.Value {
	if it.seqs != nil {
		return it.cv.values[it.seqs[it.current].AsInt()]
	} else {
		return it.cv.values[it.current]
	}
}
