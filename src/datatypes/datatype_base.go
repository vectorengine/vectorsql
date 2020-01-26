// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package datatypes

import (
	"reflect"
)

type DataTypeBase struct {
	name  string
	value reflect.Value
}

func (base *DataTypeBase) Name() string {
	return base.name
}

func (base *DataTypeBase) Type() reflect.Type {
	return base.value.Type()
}
