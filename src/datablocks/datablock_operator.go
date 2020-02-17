// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package datablocks

import (
	"expvar"
	"time"

	"base/metric"
)

func (block *DataBlock) Append(blocks ...*DataBlock) error {
	// TODO(BohuTANG): Check column
	for j := range blocks {
		appendBlock := blocks[j]
		it := appendBlock.RowIterator()
		for it.Next() {
			if err := block.WriteRow(it.Value()); err != nil {
				return err
			}
		}
	}
	return nil
}

func (block *DataBlock) Split(chunksize int) ([]*DataBlock, error) {
	defer expvar.Get(metric_datablock_split_sec).(metric.Metric).Record(time.Now())

	var blk *DataBlock
	var blocks []*DataBlock

	cols := block.Columns()
	it := block.RowIterator()

	i := 0
	for it.Next() {
		if i == 0 {
			blk = NewDataBlock(cols)
			blocks = append(blocks, blk)
		}
		if err := blk.WriteRow(it.Value()); err != nil {
			return nil, err
		}
		i++
		if i > chunksize {
			i = 0
		}
	}
	return blocks, nil
}

func (block *DataBlock) SetToLast() {
	if len(block.seqs) > 0 {
		block.seqs = block.seqs[len(block.seqs)-1:]
	}
}
