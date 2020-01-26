// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package mocks

import (
	"columns"
	"datablocks"
	"datatypes"
)

func NewBlockFromSlice(cols []columns.Column, datas ...[]interface{}) *datablocks.DataBlock {
	block := datablocks.NewDataBlock(cols)
	for _, data := range datas {
		for j, v := range data {
			_ = block.ColumnValues()[j].Insert(datatypes.ToValue(v))
		}
	}
	return block
}
