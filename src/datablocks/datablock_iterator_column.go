// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package datablocks

import (
	"columns"
	"datavalues"
)

type DataBlockColumnIterator struct {
	cv      *DataBlockValue
	seqs    []int
	rows    int
	current int
}

func newDataBlockColumnIterator(block *DataBlock, idx int) *DataBlockColumnIterator {
	rows := block.NumRows()
	cv := block.values[idx]
	return &DataBlockColumnIterator{
		cv:      cv,
		rows:    rows,
		seqs:    block.seqs,
		current: -1,
	}
}

func (it *DataBlockColumnIterator) Column() *columns.Column {
	return it.cv.column
}

func (it *DataBlockColumnIterator) Next() bool {
	it.current++
	return it.current < it.rows
}

func (it *DataBlockColumnIterator) Value() datavalues.IDataValue {
	return it.cv.values[it.seqs[it.current]]
}
