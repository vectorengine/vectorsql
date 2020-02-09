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
	mu          sync.RWMutex
	writer      io.Writer
	withNames   bool
	sampleBlock *datablocks.DataBlock
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

func (f *TSVOutputFormat) FormatPrefix() ([]byte, error) {
	if f.withNames {
		cols := f.sampleBlock.Columns()
		for i, col := range cols {
			if i != 0 {
				if _, err := f.writer.Write([]byte("\t")); err != nil {
					return nil, err
				}
			}
			if _, err := f.writer.Write([]byte(col.Name)); err != nil {
				return nil, err
			}
		}
		if _, err := f.writer.Write([]byte("\n")); err != nil {
			return nil, err
		}
	}
	return nil, nil
}

func (stream *TSVOutputFormat) Write(block *datablocks.DataBlock) error {
	stream.mu.Lock()
	defer stream.mu.Unlock()

	writer := stream.writer
	iters := block.Iterators()
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

func (f *TSVOutputFormat) FormatSuffix() (b []byte, err error) {
	return
}

func (stream *TSVOutputFormat) Name() string {
	if stream.withNames {
		return "TSVWithNames"
	}
	return "TSV"
}
