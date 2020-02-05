// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package datablocks

type IDataBlockInputStream interface {
	Name() string

	// Read next block.
	// If there are no more blocks, return nil.
	Read() (*DataBlock, error)
}
