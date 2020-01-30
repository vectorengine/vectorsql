// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package mocks

import (
	"reflect"

	"columns"
	"datablocks"
	"datatypes"
)

func NewBlockFromSlice(cols []columns.Column, datas ...[]interface{}) *datablocks.DataBlock {
	block := datablocks.NewDataBlock(cols)
	batcher := datablocks.NewBatchWriter(block.Columns())
	for _, data := range datas {
		if len(data) > 0 {
			var row []*datatypes.Value
			for i := range cols {
				row = append(row, datatypes.ToValue(data[i]))
			}
			_ = batcher.WriteRow(row...)
		}
	}
	_ = block.Write(batcher)
	return block
}

func DataBlockEqual(a *datablocks.DataBlock, b *datablocks.DataBlock) bool {
	acolumns := a.Columns()
	bcolumns := b.Columns()
	if !reflect.DeepEqual(acolumns, bcolumns) {
		return false
	}

	aiters := a.Iterators()
	biters := b.Iterators()
	for i := range aiters {
		aiter := aiters[i]
		biter := biters[i]

		for aiter.Next() {
			biter.Next()
			if cmp, err := datatypes.Compare(aiter.Value(), biter.Value()); err != nil || cmp != datatypes.Equal {
				return false
			}
		}
	}
	return true
}
