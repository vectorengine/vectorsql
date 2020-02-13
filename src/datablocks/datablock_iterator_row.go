// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package datablocks

import (
	"columns"
	"datavalues"
)

type DataBlockRowIterator struct {
	rows    int
	block   *DataBlock
	current int
}

func newDataBlockRowIterator(block *DataBlock) *DataBlockRowIterator {
	rows := block.values[0].NumRows()
	if block.seqs != nil {
		rows = len(block.seqs)
	}
	return &DataBlockRowIterator{
		rows:    rows,
		block:   block,
		current: -1,
	}
}

func (it *DataBlockRowIterator) Next() bool {
	it.current++
	return it.current < it.rows
}

func (it *DataBlockRowIterator) Column(idx int) columns.Column {
	return it.block.values[idx].column
}

func (it *DataBlockRowIterator) Value() []*datavalues.Value {
	block := it.block
	values := make([]*datavalues.Value, it.block.NumColumns())

	if block.seqs != nil {
		for i := range values {
			values[i] = block.values[i].values[block.seqs[it.current].AsInt()]
		}
	} else {
		for i := range values {
			values[i] = block.values[i].values[it.current]
		}
	}
	return values
}
