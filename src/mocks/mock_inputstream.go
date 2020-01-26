// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package mocks

import (
	"sync"

	"datablocks"
)

type MockBlockInputStream struct {
	mu     sync.RWMutex
	cursor int
	blocks []interface{}
}

func NewMockBlockInputStream(datas ...interface{}) *MockBlockInputStream {
	return &MockBlockInputStream{
		blocks: datas,
	}
}

func (stream *MockBlockInputStream) Name() string {
	return "MockBlockInputStream"
}

func (stream *MockBlockInputStream) Next() (*datablocks.DataBlock, error) {
	stream.mu.RLock()
	defer stream.mu.RUnlock()

	cursor := stream.cursor
	if stream.cursor >= len(stream.blocks) {
		return nil, nil
	}
	stream.cursor += 1
	v := stream.blocks[cursor]
	switch v := v.(type) {
	case error:
		return nil, v
	case *datablocks.DataBlock:
		return v, nil
	}
	return nil, nil
}

func (stream *MockBlockInputStream) Insert(v *datablocks.DataBlock) error {
	panic("You can't use the Insert here")
}
