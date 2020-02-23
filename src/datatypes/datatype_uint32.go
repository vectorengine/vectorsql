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
	DataTypeUInt32Name = "UInt32"
)

type UInt32DataType struct {
	DataTypeBase
}

func NewUInt32DataType() IDataType {
	return &UInt32DataType{}
}

func (datatype *UInt32DataType) Type() reflect.Type {
	return reflect.ValueOf(int32(0)).Type()
}

func (datatype *UInt32DataType) Name() string {
	return DataTypeUInt32Name
}

func (datatype *UInt32DataType) Serialize(writer *binary.Writer, v datavalues.IDataValue) error {
	if err := writer.UInt32(uint32(v.(*datavalues.ValueInt).AsInt())); err != nil {
		return errors.Wrap(err)
	}
	return nil
}

func (datatype *UInt32DataType) SerializeText(writer io.Writer, v datavalues.IDataValue) error {
	if _, err := writer.Write([]byte(fmt.Sprintf("%d", uint32(v.(*datavalues.ValueInt).AsInt())))); err != nil {
		return errors.Wrap(err)
	}
	return nil
}
