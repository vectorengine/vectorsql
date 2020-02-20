// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package datatypes

import (
	"strings"

	"base/errors"
)

type dataTypeCreator func() IDataType

var (
	table = map[string]dataTypeCreator{
		DataTypeStringName:  NewStringDataType,
		DataTypeInt32Name:   NewInt32DataType,
		DataTypeUInt32Name:  NewUInt32DataType,
		DataTypeInt64Name:   NewInt64DataType,
		DataTypeUInt64Name:  NewUInt64DataType,
		DataTypeFloat32Name: NewFloat32DataType,
		DataTypeFloat64Name: NewFloat64DataType,
	}
)

func DataTypeFactory(name string) (IDataType, error) {
	dt, ok := table[name]
	if !ok {
		if dt2, ok := table[strings.ToUpper(name)]; !ok {
			return nil, errors.Errorf("Unsupported data type:%s", name)
		} else {
			return dt2(), nil
		}
	}
	return dt(), nil
}
