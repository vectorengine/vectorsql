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

type ValueInt int64

func MakeInt(v int64) IDataValue {
	r := ValueInt(v)
	return &r
}

func ZeroInt() IDataValue {
	r := ValueInt(0)
	return &r
}

func (v *ValueInt) Size() uintptr {
	return unsafe.Sizeof(v)
}

func (v *ValueInt) String() string {
	return strconv.FormatInt(int64(*v), 10)
}

func (v *ValueInt) Type() Type {
	return TypeInt
}

func (v *ValueInt) Family() Family {
	return FamilyInt
}

func (v *ValueInt) AsInt() int64 {
	return int64(*v)
}

func (v *ValueInt) Compare(other IDataValue) (Comparison, error) {
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

func (v *ValueInt) Document() docs.Documentation {
	return docs.Text("Int")
}

func AsInt(v IDataValue) int64 {
	switch t := v.(type) {
	case *ValueInt:
		return int64(*t)
	case *ValueInt32:
		return int64(*t)
	}
	return 0
}
