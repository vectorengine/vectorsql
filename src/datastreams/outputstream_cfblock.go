// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package datastreams

import (
	"io"
	"sync"

	"datablocks"
	"dataformats"
)

type CustomFormatBlockOutputStream struct {
	mu     sync.RWMutex
	writer io.Writer

	formatName  string
	writePrefix bool
	writeSuffix bool
	header      *datablocks.DataBlock
	format      dataformats.IDataBlockOutputFormat
}

func NewCustomFormatBlockOutputStream(header *datablocks.DataBlock, writer io.Writer, formatName string) IDataBlockOutputStream {
	if formatName == "Native" {
		return NewNativeBlockOutputStream(header, writer)
	}
	return &CustomFormatBlockOutputStream{
		writer:     writer,
		formatName: formatName,
		header:     header,
		format:     dataformats.FactoryGetOutput(formatName)(writer),
	}
}

func (stream *CustomFormatBlockOutputStream) Name() string {
	return stream.formatName + "BlockOutputStream"
}

func (stream *CustomFormatBlockOutputStream) Write(block *datablocks.DataBlock) error {
	stream.mu.Lock()
	defer stream.mu.Unlock()

	if !stream.writePrefix {
		if err := stream.format.WritePrefix(); err != nil {
			return err
		}
		stream.writePrefix = true
	}

	if err := stream.format.Write(block); err != nil {
		return err
	}
	return nil
}

func (stream *CustomFormatBlockOutputStream) Finalize() error {
	if !stream.writeSuffix {
		if err := stream.format.WriteSuffix(); err != nil {
			return err
		}
		stream.writeSuffix = true
	}
	return nil
}

func (stream *CustomFormatBlockOutputStream) Close() {}

func (stream *CustomFormatBlockOutputStream) SampleBlock() *datablocks.DataBlock {
	return stream.header.Clone()
}
