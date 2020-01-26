// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package datablocks

type IDataBlockInputStream interface {
	Name() string
	Next() (*DataBlock, error)
	Insert(*DataBlock) error
}
