// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package datatypes

import (
	"reflect"

	"datavalues"

	"base/binary"
)

type IDataType interface {
	Name() string
	Type() reflect.Type
	Serialize(*binary.Writer, *datavalues.Value) error
}
