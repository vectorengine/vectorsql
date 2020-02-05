// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package datatypes

import (
	"fmt"
	"io"
	"reflect"

	"datavalues"

	"base/binary"
	"base/errors"
)

const (
	DataTypeUInt64Name = "UInt64"
)

type UInt64DataType struct {
	DataTypeBase
}

func NewUInt64DataType() IDataType {
	return &UInt64DataType{}
}

func (datatype *UInt64DataType) Type() reflect.Type {
	return reflect.ValueOf(int64(0)).Type()
}

func (datatype *UInt64DataType) Name() string {
	return DataTypeUInt64Name
}

func (datatype *UInt64DataType) Serialize(writer *binary.Writer, v *datavalues.Value) error {
	if err := writer.UInt64(uint64(v.GetInt())); err != nil {
		return errors.Wrap(err)
	}
	return nil
}

func (datatype *UInt64DataType) SerializeText(writer io.Writer, v *datavalues.Value) error {
	if _, err := writer.Write([]byte(fmt.Sprintf("%d", uint64(v.GetInt())))); err != nil {
		return errors.Wrap(err)
	}
	return nil
}
