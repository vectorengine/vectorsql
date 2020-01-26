// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package datastreams

import (
	"sync"

	"datablocks"
)

type NativeBlockInputStream struct {
	mu     sync.RWMutex
	cursor int
	blocks []*datablocks.DataBlock
}

func NewNativeBlockInputStream() datablocks.IDataBlockInputStream {
	return &NativeBlockInputStream{}
}

func (stream *NativeBlockInputStream) Name() string {
	return "NativeBlockInputStream"
}

func (stream *NativeBlockInputStream) Next() (*datablocks.DataBlock, error) {
	stream.mu.RLock()
	defer stream.mu.RUnlock()

	cursor := stream.cursor
	if stream.cursor >= len(stream.blocks) {
		return nil, nil
	}
	stream.cursor += 1
	return stream.blocks[cursor], nil
}

func (stream *NativeBlockInputStream) Insert(block *datablocks.DataBlock) error {
	stream.mu.Lock()
	defer stream.mu.Unlock()

	stream.blocks = append(stream.blocks, block)
	return nil
}
