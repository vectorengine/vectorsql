// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package datablocks

import (
	"columns"
	"datatypes"
)

type BatchWriter struct {
	values []*ColumnValue
}

func NewBatchWriter(cols []columns.Column) *BatchWriter {
	var values []*ColumnValue

	for _, col := range cols {
		cv := NewColumnValue(col)
		values = append(values, cv)
	}
	return &BatchWriter{
		values: values,
	}
}

func (bw *BatchWriter) WriteRow(row ...datatypes.Value) error {
	for i, val := range bw.values {
		val.values = append(val.values, row[i])
	}
	return nil
}
