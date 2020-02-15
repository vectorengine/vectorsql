// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package dataformats

import "datablocks"

type IDataBlockInputFormat interface {
	Name() string
}

type IDataBlockOutputFormat interface {
	Name() string
	FormatPrefix() ([]byte, error)
	Write(*datablocks.DataBlock) error
	FormatSuffix() ([]byte, error)
}
