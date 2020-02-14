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
	DataTypeFloat64Name = "Float64"
)

type Float64DataType struct {
	DataTypeBase
}

func NewFloat64DataType() IDataType {
	return &Float64DataType{}
}

func (datatype *Float64DataType) Type() reflect.Type {
	return reflect.ValueOf(int64(0)).Type()
}

func (datatype *Float64DataType) Name() string {
	return DataTypeFloat64Name
}

func (datatype *Float64DataType) Serialize(writer *binary.Writer, v *datavalues.Value) error {
	if err := writer.Float64(v.GetFloat()); err != nil {
		return errors.Wrap(err)
	}
	return nil
}

func (datatype *Float64DataType) SerializeText(writer io.Writer, v *datavalues.Value) error {
	if _, err := writer.Write([]byte(fmt.Sprintf("%v", float64(v.GetFloat())))); err != nil {
		return errors.Wrap(err)
	}
	return nil
}
