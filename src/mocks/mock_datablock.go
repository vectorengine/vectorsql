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

func NewBlockFromSlice(cols []*columns.Column, datas ...[]interface{}) *datablocks.DataBlock {
	block := datablocks.NewDataBlock(cols)
	for _, data := range datas {
		if len(data) > 0 {
			var row []datavalues.IDataValue
			for i := range cols {
				row = append(row, datavalues.ToValue(data[i]))
			}
			_ = block.WriteRow(row)
		}
	}
	return block
}

func NewSourceFromSlice(datas ...interface{}) []interface{} {
	return datas
}

func DataBlockEqual(a *datablocks.DataBlock, b *datablocks.DataBlock) bool {
	if a == nil && b == nil {
		return true
	}

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
			if cmp, err := aiter.Value().Compare(biter.Value()); err != nil || cmp != datavalues.Equal {
				return false
			}
		}
	}
	return true
}
