// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package datablocks

type IDataBlockOutputFormat interface {
	IDataBlockOutputStream

	FormatPrefix() ([]byte, error)
	FormatSuffix() ([]byte, error)
}
