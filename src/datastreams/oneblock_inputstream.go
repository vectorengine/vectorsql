// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package datastreams

import (
	"sync"

	"datablocks"
)

type OneBlockInputStream struct {
	mu      sync.RWMutex
	block   *datablocks.DataBlock
	current int
}

func NewOneBlockInputStream(block *datablocks.DataBlock) datablocks.IDataBlockInputStream {
	return &OneBlockInputStream{
		block: block,
	}
}

func (stream *OneBlockInputStream) Name() string {
	return "OneBlockInputStream"
}

func (stream *OneBlockInputStream) Read() (*datablocks.DataBlock, error) {
	stream.mu.RLock()
	defer stream.mu.RUnlock()

	if stream.current > 0 {
		return nil, nil
	}
	stream.current++
	return stream.block, nil
}
