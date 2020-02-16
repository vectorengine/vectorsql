// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package datablocks

import (
	"expvar"
	"time"

	"base/metric"
	"datavalues"
)

func (block *DataBlock) Append(blocks ...*DataBlock) error {
	// TODO(BohuTANG): Check column
	for j := range blocks {
		for i := range blocks[j].values {
			block.values[i].values = append(block.values[i].values, blocks[j].values[i].values...)
		}
	}
	return nil
}

func (block *DataBlock) Split(chunksize int) []*DataBlock {
	defer expvar.Get(metric_datablock_split_sec).(metric.Metric).Record(time.Now())

	cols := block.Columns()
	nums := block.NumRows()
	chunks := (nums / chunksize) + 1
	blocks := make([]*DataBlock, chunks)
	for i := range blocks {
		blocks[i] = NewDataBlock(cols)
	}

	for i := range cols {
		it := newDataBlockColumnIterator(block, i)
		for j := 0; j < len(blocks); j++ {
			begin := j * chunksize
			end := (j + 1) * chunksize
			if end > nums {
				end = nums
			}
			blocks[j].values[i].values = make([]*datavalues.Value, (end - begin))
			for k := begin; k < end; k++ {
				it.Next()
				blocks[j].values[i].values[k-begin] = it.Value()
			}
		}
	}
	return blocks
}

func (block *DataBlock) SetToLast() {
	if block.seqs == nil {
		block.seqs = make([]*datavalues.Value, 1)
		block.seqs[0] = datavalues.MakeInt(block.NumRows() - 1)
	} else {
		block.seqs = block.seqs[len(block.seqs)-1:]
	}
}
