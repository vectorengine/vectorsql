// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package datablocks

type IDataBlockOutputFormat interface {
	Name() string
	FormatPrefix() ([]byte, error)
	Write(*DataBlock) error
	FormatSuffix() ([]byte, error)
}
