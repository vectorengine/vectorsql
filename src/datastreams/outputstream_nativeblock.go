// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package datastreams

import (
	"io"

	"datablocks"
	"dataformats"
)

type NativeBlockOutputStream struct {
	writer io.Writer
	header *datablocks.DataBlock
	format dataformats.IDataBlockOutputFormat
}

func NewNativeBlockOutputStream(header *datablocks.DataBlock, writer io.Writer) IDataBlockOutputStream {
	return &NativeBlockOutputStream{
		header: header,
		writer: writer,
		format: dataformats.FactoryGetOutput("NativeBlock")(header, writer),
	}
}

func (stream *NativeBlockOutputStream) Name() string {
	return "NativeBlockOutputStream"
}

func (stream *NativeBlockOutputStream) Write(block *datablocks.DataBlock) error {
	return stream.format.Write(block)
}

func (stream *NativeBlockOutputStream) Finalize() error {
	return nil
}

func (stream *NativeBlockOutputStream) SampleBlock() *datablocks.DataBlock {
	return stream.header.Clone()
}
