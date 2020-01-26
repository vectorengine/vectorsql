// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package datatypes

import (
	"reflect"

	"base/binary"
	"base/errors"
)

const (
	DataTypeStringName = "String"
)

type StringDataType struct {
	DataTypeBase
}

func NewStringDataType() IDataType {
	return &StringDataType{}
}

func (datatype *StringDataType) Type() reflect.Type {
	return reflect.ValueOf(string("")).Type()
}

func (datatype *StringDataType) Name() string {
	return DataTypeStringName
}

func (datatype *StringDataType) Serialize(writer *binary.Writer, v Value) error {
	if err := writer.String(v.ToRawValue().(string)); err != nil {
		return errors.Wrap(err)
	}
	return nil
}
