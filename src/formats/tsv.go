// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package formats

import (
	"io"
	"sync"

	"datablocks"
)

type TSVOutputFormat struct {
	writer io.Writer
	mu     sync.RWMutex
	DataBlockOutputFormatBase
}

func NewTSVOutputFormat(writer io.Writer) datablocks.IDataBlockOutputFormat {
	return &TSVOutputFormat{
		writer: writer,
	}
}

func (stream *TSVOutputFormat) Write(block *datablocks.DataBlock) error {
	stream.mu.Lock()
	defer stream.mu.Unlock()

	writer := stream.writer
	iters := block.Iterators()
	for i := 0; i < block.NumRows(); i++ {
		for i, it := range iters {
			if i != 0 {
				writer.Write([]byte("\t"))
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
		writer.Write([]byte("\n"))
	}
	return nil
}

func (stream *TSVOutputFormat) Name() string {
	return "TSV"
}
