// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package datablocks

func (block *DataBlock) Limit(offset, limit int) (cutOffset, cutLimit int) {
	preRows := block.NumRows()

	st := offset
	ed := limit + offset

	if ed > preRows {
		ed = preRows
	}
	if st > preRows {
		st = preRows
	}
	block.seqs = block.seqs[st:ed]

	cutOffset += st
	cutLimit += ed - st
	return
}
