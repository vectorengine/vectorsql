// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package datavalues

import (
	"strconv"
	"unsafe"

	"base/docs"
	"base/errors"
)

type ValueInt32 int32

func MakeInt32(v int32) IDataValue {
	r := ValueInt32(v)
	return &r
}

func ZeroInt32() IDataValue {
	r := ValueInt32(0)
	return &r
}

func (v *ValueInt32) Size() uintptr {
	return unsafe.Sizeof(v)
}

func (v *ValueInt32) String() string {
	return strconv.FormatInt(int64(*v), 10)
}

func (v *ValueInt32) Type() Type {
	return TypeInt32
}

func (v *ValueInt32) Family() Family {
	return FamilyInt
}

func (v *ValueInt32) AsInt() int32 {
	return int32(*v)
}

func (v *ValueInt32) Compare(other IDataValue) (Comparison, error) {
	if !IsIntegral(other) {
		return 0, errors.Errorf("type mismatch between values, got:%v", other.Type())
	}

	a := int64(*v)
	b := AsInt(other)
	switch {
	case a > b:
		return 1, nil
	case b > a:
		return -1, nil
	default:
		return 0, nil
	}
}

func (v *ValueInt32) Document() docs.Documentation {
	return docs.Text("Int32")
}

func AsInt32(v IDataValue) int32 {
	if t, ok := v.(*ValueInt32); ok {
		return int32(*t)
	}
	return 0
}
