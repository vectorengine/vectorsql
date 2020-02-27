// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package datastreams

import (
	"datablocks"
)

type ChannelBlockInputStream struct {
	queue chan interface{}
}

func NewChannelBlockInputStream(queue chan interface{}) IDataBlockInputStream {
	return &ChannelBlockInputStream{
		queue: queue,
	}
}

func (stream *ChannelBlockInputStream) Name() string {
	return "ChannelBlockInputStream"
}

func (stream *ChannelBlockInputStream) Read() (*datablocks.DataBlock, error) {
	val, ok := <-stream.queue
	if ok {
		switch t := val.(type) {
		case error:
			return nil, t
		case *datablocks.DataBlock:
			return t, nil
		}
	}
	return nil, nil
}
