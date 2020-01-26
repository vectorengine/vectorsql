// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package datastreams

import (
	"sync"

	"datablocks"

	"base/binary"
	"base/errors"
)

type NativeBlockOutputStream struct {
	mu     sync.RWMutex
	writer *binary.Writer
}

func NewNativeBlockOutputStream(writer *binary.Writer) datablocks.IDataBlockOutputStream {
	return &NativeBlockOutputStream{writer: writer}
}

func (stream *NativeBlockOutputStream) Name() string {
	return "NativeBlockOutputStream"
}

func (stream *NativeBlockOutputStream) Write(block *datablocks.DataBlock) error {
	stream.mu.Lock()
	defer stream.mu.Unlock()
	writer := stream.writer

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
	for _, v := range block.Values() {
		datatype := v.Column().DataType

		// Column name.
		if err := writer.String(v.Column().Name); err != nil {
			return errors.Wrap(err)
		}

		// Datatype name.
		if err := writer.String(datatype.Name()); err != nil {
			return errors.Wrap(err)
		}

		// Data serialize.
		for _, val := range v.Values() {
			if err := datatype.Serialize(writer, val); err != nil {
				return err
			}
		}
	}
	return nil
}
