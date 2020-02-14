// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package columns

import (
	"datatypes"
)

type Column struct {
	Name     string
	DataType datatypes.IDataType
}

func NewColumn(name string, datatype datatypes.IDataType) *Column {
	return &Column{
		Name:     name,
		DataType: datatype,
	}
}
