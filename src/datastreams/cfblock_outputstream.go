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

	format datablocks.IDataBlockOutputFormat
}

func NewCustomFormatBlockOutputStream(header *datablocks.DataBlock, writer io.Writer, formatName string) datablocks.IDataBlockOutputStream {
	if formatName == "Native" {
		return NewNativeBlockOutputStream(writer)
	}
	return &CustomFormatBlockOutputStream{
		writer:     writer,
		formatName: formatName,
		format:     dataformats.FactoryGetOutput(formatName)(header, writer),
	}
}

func (stream *CustomFormatBlockOutputStream) Name() string {
	return stream.formatName + "BlockOutputStream"
}

func (stream *CustomFormatBlockOutputStream) Write(block *datablocks.DataBlock) error {
	stream.mu.Lock()
	defer stream.mu.Unlock()

	if !stream.writePrefix {
		if _, err := stream.format.FormatPrefix(); err != nil {
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
		if _, err := stream.format.FormatSuffix(); err != nil {
			return err
		}
		stream.writeSuffix = true
	}
	return nil
}
