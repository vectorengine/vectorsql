// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package dataformats

import (
	"datablocks"
)

type IDataBlockInputFormat interface {
	ReadPrefix() error
	Read() (*datablocks.DataBlock, error)
	ReadSuffix() error
}

type IDataBlockOutputFormat interface {
	WritePrefix() error
	Write(*datablocks.DataBlock) error
	WriteSuffix() error
}
