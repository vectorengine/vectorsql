// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package datatypes

import (
	"fmt"
	"io"
	"reflect"

	"base/binary"
	"base/errors"
	"datavalues"
)

const (
	DataTypeInt64Name = "Int64"
)

type Int64DataType struct {
	DataTypeBase
}

func NewInt64DataType() IDataType {
	return &Int64DataType{}
}

func (datatype *Int64DataType) Type() reflect.Type {
	return reflect.ValueOf(int64(0)).Type()
}

func (datatype *Int64DataType) Name() string {
	return DataTypeInt64Name
}

func (datatype *Int64DataType) Serialize(writer *binary.Writer, v datavalues.IDataValue) error {
	if err := writer.Int64(datavalues.AsInt(v)); err != nil {
		return errors.Wrap(err)
	}
	return nil
}

func (datatype *Int64DataType) SerializeText(writer io.Writer, v datavalues.IDataValue) error {
	if _, err := writer.Write([]byte(fmt.Sprintf("%v", datavalues.AsInt(v)))); err != nil {
		return errors.Wrap(err)
	}
	return nil
}
