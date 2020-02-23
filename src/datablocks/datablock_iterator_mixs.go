// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package datablocks

import (
	"base/errors"
	"columns"
	"datavalues"
)

type DataBlockMixsIterator struct {
	rows    int
	block   *DataBlock
	indexs  []int
	current int
}

type idxName struct {
	idx  int
	name string
}

func newDataBlockColsRowIterator(block *DataBlock, cols []string) (*DataBlockMixsIterator, error) {
	maps := make(map[string]*idxName)
	for i, cv := range block.values {
		maps[cv.column.Name] = &idxName{
			idx:  i,
			name: cv.column.Name,
		}
	}

	idxs := make([]int, len(cols))
	for i, name := range cols {
		if v, ok := maps[name]; !ok {
			return nil, errors.Errorf("Can't find column:%v", name)
		} else {
			idxs[i] = v.idx
		}
	}
	return &DataBlockMixsIterator{
		rows:    len(block.seqs),
		block:   block,
		indexs:  idxs,
		current: -1,
	}, nil
}

func (it *DataBlockMixsIterator) Next() bool {
	it.current++
	return it.current < it.rows
}

func (it *DataBlockMixsIterator) Last() []datavalues.IDataValue {
	it.current = it.rows - 1
	return it.Value()
}

func (it *DataBlockMixsIterator) Column(idx int) *columns.Column {
	return it.block.values[it.indexs[idx]].column
}

func (it *DataBlockMixsIterator) Value() []datavalues.IDataValue {
	block := it.block
	values := make([]datavalues.IDataValue, len(it.indexs))

	for i := range it.indexs {
		values[i] = block.values[it.indexs[i]].values[block.seqs[it.current]]
	}
	return values
}
