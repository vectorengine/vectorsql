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

func NewMockBlockInputStream(datas []interface{}) *MockBlockInputStream {
	return &MockBlockInputStream{
		blocks: datas,
	}
}

func (stream *MockBlockInputStream) Name() string {
	return "MockBlockInputStream"
}

func (stream *MockBlockInputStream) Read() (*datablocks.DataBlock, error) {
	stream.mu.RLock()
	defer stream.mu.RUnlock()

	cursor := stream.cursor
	if cursor >= len(stream.blocks) {
		return nil, nil
	}
	v := stream.blocks[cursor]
	stream.cursor++
	switch v := v.(type) {
	case error:
		return nil, v
	case *datablocks.DataBlock:
		return v, nil
	default:
		panic(v)
	}
}

func (stream *MockBlockInputStream) Insert(v *datablocks.DataBlock) error {
	panic("You can't use the Insert here")
}
