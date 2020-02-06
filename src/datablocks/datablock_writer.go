// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package datablocks

import (
	"columns"
	"datavalues"
)

type BatchWriter struct {
	values []*DataBlockValue
	rows   int
}

func NewBatchWriter(cols []columns.Column) *BatchWriter {
	var values []*DataBlockValue

	for _, col := range cols {
		cv := NewDataBlockValue(col)
		values = append(values, cv)
	}
	return &BatchWriter{
		values: values,
	}
}

func (bw *BatchWriter) WriteRow(row ...*datavalues.Value) error {
	for i, val := range bw.values {
		val.values = append(val.values, row[i])
	}
	bw.rows++
	return nil
}
