// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package datatypes

import (
	"base/binary"
	"base/errors"
	"datavalues"
	"io"
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

func (datatype *StringDataType) Name() string {
	return DataTypeStringName
}

func (datatype *StringDataType) Serialize(writer *binary.Writer, v datavalues.IDataValue) error {
	if err := writer.String(datavalues.AsString(v)); err != nil {
		return errors.Wrap(err)
	}
	return nil
}

func (datatype *StringDataType) SerializeText(writer io.Writer, v datavalues.IDataValue) error {
	if _, err := writer.Write([]byte(datavalues.AsString(v))); err != nil {
		return errors.Wrap(err)
	}
	return nil
}

func (datatype *StringDataType) Deserialize(reader *binary.Reader) (datavalues.IDataValue, error) {
	if res, err := reader.String(); err != nil {
		return nil, errors.Wrap(err)
	} else {
		return datavalues.MakeString(res), nil
	}
}
