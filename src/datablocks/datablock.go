// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package datablocks

import (
	"columns"
	"datatypes"

	"base/errors"
)

type DataBlock struct {
	info      *DataBlockInfo
	seqs      []*datatypes.Value
	values    []*ColumnValue
	immutable bool
	valuesmap map[string]*ColumnValue
}

func NewDataBlock(cols []columns.Column) *DataBlock {
	var values []*ColumnValue
	valuesmap := make(map[string]*ColumnValue)

	for _, col := range cols {
		cv := NewColumnValue(col)
		valuesmap[col.Name] = cv
		values = append(values, cv)
	}
	return &DataBlock{
		info:      &DataBlockInfo{},
		values:    values,
		valuesmap: valuesmap,
	}
}

func (block *DataBlock) Info() *DataBlockInfo {
	return block.info
}

func (block *DataBlock) NumRows() int {
	if block.seqs != nil {
		return len(block.seqs)
	} else {
		return block.values[0].NumRows()
	}
}

func (block *DataBlock) NumColumns() int {
	return len(block.values)
}

func (block *DataBlock) Columns() []columns.Column {
	var cols []columns.Column

	for _, cv := range block.values {
		cols = append(cols, cv.column)
	}
	return cols
}

func (block *DataBlock) Iterator(name string) (*DataBlockIterator, error) {
	cv, ok := block.valuesmap[name]
	if !ok {
		return nil, errors.Errorf("Can't find column:%v", name)
	}
	return newDataBlockIterator(block.seqs, cv), nil
}

func (block *DataBlock) Iterators() []*DataBlockIterator {
	var iterators []*DataBlockIterator

	for _, cv := range block.values {
		iter := newDataBlockIterator(block.seqs, cv)
		iterators = append(iterators, iter)
	}
	return iterators
}

func (block *DataBlock) Write(batcher *BatchWriter) error {
	if block.immutable {
		return errors.New("Can't write, block is immutable")
	}

	cols := batcher.values
	for _, col := range cols {
		if _, ok := block.valuesmap[col.column.Name]; !ok {
			return errors.Errorf("Can't find column:%v", col)
		}
	}

	for _, col := range cols {
		cv := block.valuesmap[col.column.Name]
		cv.values = append(cv.values, col.values...)
	}
	return nil
}

func (block *DataBlock) setSeqs(seqs []*datatypes.Value) {
	block.seqs = seqs
	block.immutable = true
}
