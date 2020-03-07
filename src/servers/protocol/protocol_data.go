// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package protocol

import (
	"base/binary"
	"datablocks"
	"datastreams"
)

func ReadDataRequest(reader *binary.Reader) (*datablocks.DataBlock, error) {
	stream := datastreams.NewNativeBlockInputStream(reader)
	return stream.Read()
}
