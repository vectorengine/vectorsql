// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package datatypes

import (
	"fmt"
	"io"

	"base/binary"
	"base/errors"
	"datavalues"
)

const (
	DataTypeInt32Name = "Int32"
)

type Int32DataType struct {
}

func NewInt32DataType() IDataType {
	return &Int32DataType{}
}

func (datatype *Int32DataType) Name() string {
	return DataTypeInt32Name
}

func (datatype *Int32DataType) Serialize(writer *binary.Writer, v datavalues.IDataValue) error {
	return writer.Int32(int32(datavalues.AsInt(v)))
}

func (datatype *Int32DataType) SerializeText(writer io.Writer, v datavalues.IDataValue) error {
	_, err := writer.Write([]byte(fmt.Sprintf("%d", int32(datavalues.AsInt(v)))))
	return err
}

func (datatype *Int32DataType) Deserialize(reader *binary.Reader) (datavalues.IDataValue, error) {
	if res, err := reader.Int32(); err != nil {
		return nil, errors.Wrap(err)
	} else {
		return datavalues.MakeInt32(res), nil
	}
}
