// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package datablocks

import (
	"expvar"
	"time"

	"base/metric"
	"columns"
	"datatypes"
	"datavalues"
	"expressions"
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

func BuildOneBlockFromExpressions(exprs []expressions.IExpression) (*DataBlock, error) {
	var err error
	var dtype datatypes.IDataType
	row := make([]datavalues.IDataValue, 0, len(exprs))
	column := make([]*columns.Column, len(exprs))

	for i, expr := range exprs {
		res := expr.Result()
		if res != nil {
			dtype, err = datatypes.GetDataTypeByValue(res)
			if err != nil {
				return nil, err
			}
			row = append(row, res)
		} else {
			dtype = datatypes.NewStringDataType()
		}
		column[i] = columns.NewColumn(expr.String(), dtype)
	}

	group := NewDataBlock(column)
	if len(row) > 0 {
		if err := group.WriteRow(row); err != nil {
			return nil, err
		}
	}
	return group, nil
}
