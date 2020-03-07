// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package dataformats

import (
	"io"
	"sync"

	"base/binary"
	"base/errors"
	"datablocks"
)

type NativeBlockOutputFormat struct {
	mu          sync.RWMutex
	writer      io.Writer
	sampleBlock *datablocks.DataBlock
}

func NewNativeBlockOutputFormat(sampleBlock *datablocks.DataBlock, writer io.Writer) IDataBlockOutputFormat {
	return &NativeBlockOutputFormat{
		writer:      writer,
		sampleBlock: sampleBlock,
	}
}

func (format *NativeBlockOutputFormat) WritePrefix() error {
	return nil
}

func (format *NativeBlockOutputFormat) Write(block *datablocks.DataBlock) error {
	format.mu.Lock()
	defer format.mu.Unlock()
	writer := binary.NewWriter(format.writer)

	// Block info.
	info := block.Info()
	if err := info.Write(writer); err != nil {
		return err
	}

	// NumColumns.
	if err := writer.Uvarint(uint64(block.NumColumns())); err != nil {
		return errors.Wrap(err)
	}
	// NumRows.
	if err := writer.Uvarint(uint64(block.NumRows())); err != nil {
		return errors.Wrap(err)
	}

	// Values.
	for _, it := range block.ColumnIterators() {
		column := it.Column()
		datatype := column.DataType

		// Column name.
		if err := writer.String(column.Name); err != nil {
			return errors.Wrap(err)
		}

		// Datatype name.
		if err := writer.String(datatype.Name()); err != nil {
			return errors.Wrap(err)
		}

		for it.Next() {
			// Data serialize.
			if err := datatype.Serialize(writer, it.Value()); err != nil {
				return err
			}
		}
	}
	return nil
}

func (format *NativeBlockOutputFormat) WriteSuffix() error {
	return nil
}
