// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package dataformats

import (
	"io"
	"sync"

	"datablocks"
)

type TSVOutputFormat struct {
	mu        sync.RWMutex
	writer    io.Writer
	withNames bool
}

func NewTSVOutputFormat(writer io.Writer) IDataBlockOutputFormat {
	return &TSVOutputFormat{
		writer:    writer,
		withNames: false,
	}
}

func NewTSVWithNamesOutputFormat(writer io.Writer) IDataBlockOutputFormat {
	return &TSVOutputFormat{
		writer:    writer,
		withNames: true,
	}
}

func (format *TSVOutputFormat) WritePrefix() error {
	return nil
}

func (format *TSVOutputFormat) Write(block *datablocks.DataBlock) error {
	format.mu.Lock()
	defer format.mu.Unlock()

	writer := format.writer
	iters := block.ColumnIterators()
	for i := 0; i < block.NumRows(); i++ {
		for i, it := range iters {
			if i != 0 {
				if _, err := writer.Write([]byte("\t")); err != nil {
					return err
				}
			}
			column := it.Column()
			datatype := column.DataType

			if it.Next() {
				// Data serialize.
				if err := datatype.SerializeText(writer, it.Value()); err != nil {
					return err
				}
			}
		}
		if _, err := writer.Write([]byte("\n")); err != nil {
			return err
		}
	}
	return nil
}

func (format *TSVOutputFormat) WriteSuffix() error {
	return nil
}
