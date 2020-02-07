// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package datablocks

import (
	"columns"
	"datavalues"

	"base/errors"
)

type DataBlock struct {
	info      *DataBlockInfo
	seqs      []*datavalues.Value
	values    []*DataBlockValue
	indexmap  map[string]int
	valuesmap map[string]*DataBlockValue

	immutable bool
}

func NewDataBlock(cols []columns.Column) *DataBlock {
	block := &DataBlock{
		info:      &DataBlockInfo{},
		values:    []*DataBlockValue{},
		indexmap:  make(map[string]int),
		valuesmap: make(map[string]*DataBlockValue),
	}

	for _, col := range cols {
		block.InsertColumn(col)
	}
	return block
}

// Clone a sample block
func (block *DataBlock) Clone() *DataBlock {
	return NewDataBlock(block.Columns())
}

func (block *DataBlock) InsertColumn(col columns.Column) {
	cv := NewDataBlockValue(col)
	idx := len(block.indexmap)
	block.indexmap[col.Name] = idx
	block.valuesmap[col.Name] = cv

	block.values = append(block.values, cv)
}

func (block *DataBlock) setSeqs(seqs []*datavalues.Value) {
	block.seqs = seqs
	block.immutable = true
}

func (block *DataBlock) Seqs() []*datavalues.Value {
	return block.seqs
}

func (block *DataBlock) Info() *DataBlockInfo {
	return block.info
}

func (block *DataBlock) NumRows() int {
	if block.seqs != nil {
		return len(block.seqs)
	} else if len(block.values) == 0 {
		return 0
	}
	return block.values[0].NumRows()
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

func (block *DataBlock) Column(name string) (columns.Column, error) {
	cv, ok := block.valuesmap[name]
	if !ok {
		return columns.Column{}, errors.Errorf("Can't find column:%v", name)
	}
	return cv.column, nil
}

func (block *DataBlock) Iterator(name string) (*DataBlockColumnIterator, error) {
	idx := 0
	if _, ok := block.valuesmap[name]; !ok {
		return nil, errors.Errorf("Can't find column:%v", name)
	}
	idx, ok := block.indexmap[name]
	if !ok {
		return nil, errors.Errorf("Can't find column:%v", name)
	}
	return newDataBlockColumnIterator(block, idx), nil
}

func (block *DataBlock) Iterators() []*DataBlockColumnIterator {
	var iterators []*DataBlockColumnIterator

	for i := range block.values {
		iter := newDataBlockColumnIterator(block, i)
		iterators = append(iterators, iter)
	}
	return iterators
}

func (block *DataBlock) First(name string) (*datavalues.Value, error) {
	it, err := block.Iterator(name)
	if err != nil {
		return nil, errors.Errorf("Can't find column:%v", name)
	}
	if it.Next() {
		return it.Value(), nil
	}
	return nil, nil
}

func (block *DataBlock) WriteBatch(batcher *BatchWriter) error {
	if block.immutable {
		return errors.New("Block is immutable")
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

func (block *DataBlock) WriteRow(values []*datavalues.Value) error {
	cols := block.NumColumns()
	if len(values) != cols {
		return errors.Errorf("Can't append row, expect column length:%v", cols)
	}

	for i := 0; i < cols; i++ {
		block.values[i].values = append(block.values[i].values, values[i])
	}
	return nil
}
