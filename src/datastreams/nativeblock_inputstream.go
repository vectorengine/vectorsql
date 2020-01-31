// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package datastreams

import (
	"datablocks"
)

type NativeBlockInputStream struct {
}

func NewNativeBlockInputStream() datablocks.IDataBlockInputStream {
	return &NativeBlockInputStream{}
}

func (stream *NativeBlockInputStream) Name() string {
	return "NativeBlockInputStream"
}

func (stream *NativeBlockInputStream) Read() (*datablocks.DataBlock, error) {
	// TODO
	return nil, nil
}
