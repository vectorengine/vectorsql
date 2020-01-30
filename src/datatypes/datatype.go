// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package datatypes

import (
	"reflect"

	"base/binary"
)

type IDataType interface {
	Name() string
	Type() reflect.Type
	Serialize(*binary.Writer, *Value) error
}
