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
	info   *DataBlockInfo
	values []*columns.ColumnValue
}

func NewDataBlock(cols []columns.Column) *DataBlock {
	var values []*columns.ColumnValue
	for _, col := range cols {
		values = append(values, columns.NewColumnValue(col))
	}
	return &DataBlock{
		info:   &DataBlockInfo{},
		values: values,
	}
}

func (block *DataBlock) Info() *DataBlockInfo {
	return block.info
}

func (block *DataBlock) NumRows() int {
	return block.values[0].NumRows()
}

func (block *DataBlock) NumColumns() int {
	return len(block.values)
}

func (block *DataBlock) Columns() []columns.Column {
	var cols []columns.Column
	for _, cv := range block.values {
		cols = append(cols, cv.Column)
	}
	return cols
}

func (block *DataBlock) ColumnValues() []*columns.ColumnValue {
	var vals []*columns.ColumnValue
	return append(vals, block.values...)
}

func (block *DataBlock) Column(name string) (*columns.ColumnValue, error) {
	for _, cv := range block.values {
		if cv.Column.Name == name {
			return cv, nil
		}
	}
	return nil, errors.Errorf("Can't find column:%v", name)
}

func (block *DataBlock) Values() []*columns.ColumnValue {
	return block.values
}

func (block *DataBlock) Insert(col string, v datatypes.Value) error {
	cv, err := block.Column(col)
	if err != nil {
		return errors.Errorf("Can't find column:%v", col)
	}
	return cv.Insert(v)
}
