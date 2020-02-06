// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package datablocks

import (
	"columns"
	"datavalues"
)

type DataBlockColumnIterator struct {
	cv   *DataBlockValue
	seqs []*datavalues.Value

	current int
	end     int
}

func newDataBlockColumnIterator(block *DataBlock, idx int) *DataBlockColumnIterator {
	cv := block.values[idx]
	seqs := block.Seqs()

	return &DataBlockColumnIterator{
		cv:      cv,
		seqs:    seqs,
		current: block.start - 1,
		end:     block.end,
	}
}

func (it *DataBlockColumnIterator) Column() columns.Column {
	return it.cv.column
}

func (it *DataBlockColumnIterator) Next() bool {
	it.current++
	return it.current < it.end
}

func (it *DataBlockColumnIterator) Value() *datavalues.Value {
	if it.seqs != nil {
		return it.cv.values[it.seqs[it.current].AsInt()]
	} else {
		return it.cv.values[it.current]
	}
}
