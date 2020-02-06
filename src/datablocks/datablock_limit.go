// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package datablocks

func (block *DataBlock) Limit(offset, limit int) (cutOffset, cutLimit int) {
	preRows := block.NumRows()
	if block.start+offset < block.end {
		block.start += offset
		cutOffset += offset
	} else {
		block.start = block.end
		cutOffset += preRows
	}

	if block.end-block.start > limit {
		block.end = block.start + limit
	}
	cutLimit += block.NumRows()
	return
}
