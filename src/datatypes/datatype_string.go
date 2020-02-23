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

func (datatype *StringDataType) Serialize(writer *binary.Writer, v datavalues.IDataValue) error {
	if err := writer.String(v.(*datavalues.ValueString).AsString()); err != nil {
		return errors.Wrap(err)
	}
	return nil
}

func (datatype *StringDataType) SerializeText(writer io.Writer, v datavalues.IDataValue) error {
	if _, err := writer.Write([]byte(v.(*datavalues.ValueString).AsString())); err != nil {
		return errors.Wrap(err)
	}
	return nil
}
