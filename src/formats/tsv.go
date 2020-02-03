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
	DataBlockOutputFormatBase

	sampleBlock *datablocks.DataBlock
	writer      io.Writer
	withNames   bool
	mu          sync.RWMutex
}

func NewTSVOutputFormat(sampleBlock *datablocks.DataBlock, writer io.Writer) datablocks.IDataBlockOutputFormat {
	return &TSVOutputFormat{
		writer:    writer,
		withNames: false,
	}
}

func NewTSVWithNamesOutputFormat(sampleBlock *datablocks.DataBlock, writer io.Writer) datablocks.IDataBlockOutputFormat {
	return &TSVOutputFormat{
		writer:    writer,
		withNames: true,
	}
}

func (f *TSVOutputFormat) FormatPrefix() (b []byte, err error) {
	if f.withNames {
		cols := f.sampleBlock.Columns()
		for i, col := range cols {
			if i != 0 {
				f.writer.Write([]byte("\t"))
			}
			f.writer.Write([]byte(col.Name))
		}
		f.writer.Write([]byte("\n"))
	}
	return
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
