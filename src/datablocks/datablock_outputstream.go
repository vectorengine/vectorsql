// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package datablocks

import ()

type IDataBlockOutputStream interface {
	Name() string
	Write(*DataBlock) error
}
