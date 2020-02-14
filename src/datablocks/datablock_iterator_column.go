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
	seqs    []*datavalues.Value
	rows    int
	current int
}

func newDataBlockColumnIterator(block *DataBlock, idx int) *DataBlockColumnIterator {
	cv := block.values[idx]
	seqs := block.Seqs()
	rows := cv.NumRows()
	if seqs != nil {
		rows = len(seqs)
	}
	return &DataBlockColumnIterator{
		cv:      cv,
		rows:    rows,
		seqs:    seqs,
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

func (it *DataBlockColumnIterator) Value() *datavalues.Value {
	if it.seqs != nil {
		return it.cv.values[it.seqs[it.current].AsInt()]
	} else {
		return it.cv.values[it.current]
	}
}
