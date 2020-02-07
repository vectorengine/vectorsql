// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package datablocks

import "datavalues"

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
	if block.seqs == nil {
		block.seqs = make([]*datavalues.Value, preRows)
		for i := 0; i < len(block.seqs); i++ {
			block.seqs[i] = datavalues.MakeInt(i)
		}
	}
	block.seqs = block.seqs[st:ed]

	cutOffset += st
	cutLimit += ed - st
	return
}
