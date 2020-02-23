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

type ValueInt struct {
	dint int64
}

func MakeInt(v int64) IDataValue {
	return &ValueInt{dint: v}
}

func ZeroInt() IDataValue {
	return &ValueInt{dint: int64(0)}
}

func (v *ValueInt) Size() uintptr {
	return unsafe.Sizeof(*v)
}

func (v *ValueInt) Show() []byte {
	return strconv.AppendInt(nil, v.dint, 10)
}

func (v *ValueInt) GetType() Type {
	return TypeInt
}

func (v *ValueInt) AsInt() int64 {
	return v.dint
}

func (v *ValueInt) Compare(other IDataValue) (Comparison, error) {
	if other.GetType() != TypeInt {
		return 0, errors.Errorf("type mismatch between values")
	}

	a := v.dint
	b := other.(*ValueInt).dint
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
