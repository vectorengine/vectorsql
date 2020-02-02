// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package datablocks

import ()

func Append(blocks ...*DataBlock) (*DataBlock, error) {
	// TODO(BohuTANG): Check column
	block := NewDataBlock(blocks[0].Columns())
	for j := range blocks {
		for i := range blocks[j].values {
			block.values[i].values = append(block.values[i].values, blocks[j].values[i].values...)
		}
	}
	return block, nil
}
