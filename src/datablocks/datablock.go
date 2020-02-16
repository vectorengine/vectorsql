// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package datablocks

import (
	"base/errors"
	"columns"
	"datavalues"
)

type DataBlock struct {
	info      *DataBlockInfo
	seqs      []*datavalues.Value
	values    []*DataBlockValue
	valuesmap map[string]*DataBlockValue
	immutable bool
}

func NewDataBlock(cols []*columns.Column) *DataBlock {
	block := &DataBlock{
		info:      &DataBlockInfo{},
		values:    make([]*DataBlockValue, len(cols)),
		valuesmap: make(map[string]*DataBlockValue, len(cols)),
	}

	for i, col := range cols {
		cv := NewDataBlockValue(col)
		block.values[i] = cv
		block.valuesmap[col.Name] = cv
	}
	return block
}

// Clone a sample block
func (block *DataBlock) Clone() *DataBlock {
	return NewDataBlock(block.Columns())
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

func (block *DataBlock) Columns() []*columns.Column {
	cols := make([]*columns.Column, len(block.values))
	for i, cv := range block.values {
		cols[i] = cv.column
	}
	return cols
}

func (block *DataBlock) Column(name string) (*columns.Column, error) {
	cv, ok := block.valuesmap[name]
	if !ok {
		return nil, errors.Errorf("Can't find column:%v", name)
	}
	return cv.column, nil
}

func (block *DataBlock) DataBlockValue(name string) (*DataBlockValue, error) {
	if block.valuesmap != nil {
		if cv, ok := block.valuesmap[name]; ok {
			return cv, nil
		}
	} else {
		for _, cv := range block.values {
			if cv.column.Name == name {
				return cv, nil
			}
		}
	}
	return nil, errors.Errorf("Can't find column:%v", name)
}

func (block *DataBlock) RowIterator() *DataBlockRowIterator {
	return newDataBlockRowIterator(block)
}

func (block *DataBlock) ColumnIterator(name string) (*DataBlockColumnIterator, error) {
	for i, v := range block.values {
		if v.column.Name == name {
			return newDataBlockColumnIterator(block, i), nil
		}
	}
	return nil, errors.Errorf("Can't find column:%v", name)
}

func (block *DataBlock) ColumnIterators() []*DataBlockColumnIterator {
	var iterators []*DataBlockColumnIterator

	for i := range block.values {
		iter := newDataBlockColumnIterator(block, i)
		iterators = append(iterators, iter)
	}
	return iterators
}

func (block *DataBlock) WriteRow(values []*datavalues.Value) error {
	if block.immutable {
		return errors.New("Block is immutable")
	}

	cols := block.NumColumns()
	if len(values) != cols {
		return errors.Errorf("Can't append row, expect column length:%v", cols)
	}

	for i := 0; i < cols; i++ {
		block.values[i].values = append(block.values[i].values, values[i])
	}
	return nil
}
