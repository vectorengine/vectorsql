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
	return &DataBlockRowIterator{
		rows:    len(block.seqs),
		block:   block,
		current: -1,
	}
}

func (it *DataBlockRowIterator) Next() bool {
	it.current++
	return it.current < it.rows
}

func (it *DataBlockRowIterator) Last() []*datavalues.Value {
	it.current = it.rows - 1
	return it.Value()
}

func (it *DataBlockRowIterator) Column(idx int) *columns.Column {
	return it.block.values[idx].column
}

func (it *DataBlockRowIterator) Value() []*datavalues.Value {
	block := it.block
	values := make([]*datavalues.Value, it.block.NumColumns())

	for i := range values {
		values[i] = block.values[i].values[block.seqs[it.current]]
	}
	return values
}
