// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package datatypes

import (
	"io"
	"reflect"

	"datavalues"

	"base/binary"
	"base/errors"
)

type IDataType interface {
	Name() string
	Type() reflect.Type
	Serialize(*binary.Writer, *datavalues.Value) error
	SerializeText(io.Writer, *datavalues.Value) error
}

func GetDataTypeByValue(val *datavalues.Value) (IDataType, error) {
	switch val.GetType() {
	case datavalues.TypeString:
		return NewStringDataType(), nil
	case datavalues.TypeFloat:
		return NewFloat64DataType(), nil
	case datavalues.TypeInt:
		return NewInt32DataType(), nil
	default:
		return nil, errors.Errorf("Unsupported value type:%v", val.GetType())
	}
}
