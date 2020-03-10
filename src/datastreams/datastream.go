// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package datastreams

import (
	"datablocks"
)

type IDataBlockInputStream interface {
	Name() string

	// Read next block.
	// If there are no more blocks, return nil.
	Read() (*datablocks.DataBlock, error)
	Close()
}

type IDataBlockOutputStream interface {
	Name() string
	Write(*datablocks.DataBlock) error
	Finalize() error
	Close()
	SampleBlock() *datablocks.DataBlock
}
