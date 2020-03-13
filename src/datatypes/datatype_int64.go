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
	DataTypeInt64Name = "Int64"
)

type Int64DataType struct {
}

func NewInt64DataType() IDataType {
	return &Int64DataType{}
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

func (datatype *Int64DataType) Deserialize(reader *binary.Reader) (datavalues.IDataValue, error) {
	if res, err := reader.Int64(); err != nil {
		return nil, errors.Wrap(err)
	} else {
		return datavalues.ToValue(res), nil
	}
}
