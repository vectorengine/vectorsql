// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package datastreams

import (
	"fmt"
	"sync"

	"datablocks"
)

type OneBlockInputStream struct {
	mu      sync.RWMutex
	blocks  []*datablocks.DataBlock
	current int
}

func NewOneBlockInputStream(blocks ...*datablocks.DataBlock) IDataBlockInputStream {
	return &OneBlockInputStream{
		blocks: blocks,
	}
}

func (stream *OneBlockInputStream) Name() string {
	return "OneBlockInputStream"
}

func (stream *OneBlockInputStream) Read() (*datablocks.DataBlock, error) {
	stream.mu.RLock()
	defer stream.mu.RUnlock()

	if stream.current >= len(stream.blocks) {
		return nil, nil
	}
	block := stream.blocks[stream.current]
	stream.current++
	fmt.Printf("OneBlockInputStream->Block:%v\n", block.NumRows())
	return block, nil
}
