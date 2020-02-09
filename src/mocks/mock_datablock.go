// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package mocks

import (
	"reflect"

	"columns"
	"datablocks"
	"datavalues"
)

func NewBlockFromSlice(cols []columns.Column, datas ...[]interface{}) *datablocks.DataBlock {
	block := datablocks.NewDataBlock(cols)
	batcher := datablocks.NewBatchWriter(block.Columns())
	for _, data := range datas {
		if len(data) > 0 {
			var row []*datavalues.Value
			for i := range cols {
				row = append(row, datavalues.ToValue(data[i]))
			}
			_ = batcher.WriteRow(row...)
		}
	}
	_ = block.WriteBatch(batcher)
	return block
}

func DataBlockEqual(a *datablocks.DataBlock, b *datablocks.DataBlock) bool {
	acolumns := a.Columns()
	bcolumns := b.Columns()
	if !reflect.DeepEqual(acolumns, bcolumns) {
		return false
	}

	aiters := a.ColumnIterators()
	biters := b.ColumnIterators()
	for i := range aiters {
		aiter := aiters[i]
		biter := biters[i]

		for aiter.Next() {
			biter.Next()
			if cmp, err := datavalues.Compare(aiter.Value(), biter.Value()); err != nil || cmp != datavalues.Equal {
				return false
			}
		}
	}
	return true
}
