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
	DataTypeUInt32Name = "UInt32"
)

type UInt32DataType struct {
}

func NewUInt32DataType() IDataType {
	return &UInt32DataType{}
}

func (datatype *UInt32DataType) Name() string {
	return DataTypeUInt32Name
}

func (datatype *UInt32DataType) Serialize(writer *binary.Writer, v datavalues.IDataValue) error {
	return writer.UInt32(uint32(datavalues.AsInt(v)))
}

func (datatype *UInt32DataType) SerializeText(writer io.Writer, v datavalues.IDataValue) error {
	_, err := writer.Write([]byte(fmt.Sprintf("%d", uint32(datavalues.AsInt(v)))))
	return err
}

func (datatype *UInt32DataType) Deserialize(reader *binary.Reader) (datavalues.IDataValue, error) {
	if res, err := reader.UInt32(); err != nil {
		return nil, errors.Wrap(err)
	} else {
		return datavalues.ToValue(res), nil
	}
}
