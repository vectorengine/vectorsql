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

type ValueFloat struct {
	dfloat float64
}

func MakeFloat(v float64) IDataValue {
	return &ValueFloat{dfloat: v}
}

func ZeroFloat() IDataValue {
	return &ValueFloat{dfloat: 0}
}

func (v *ValueFloat) Size() uintptr {
	return unsafe.Sizeof(*v)
}

func (v *ValueFloat) Show() []byte {
	return strconv.AppendFloat(nil, v.dfloat, 'g', -1, 64)
}

func (v *ValueFloat) GetType() Type {
	return TypeFloat
}

func (v *ValueFloat) AsFloat() float64 {
	return v.dfloat
}

func (v *ValueFloat) Compare(other IDataValue) (Comparison, error) {
	if other.GetType() != TypeFloat {
		return 0, errors.Errorf("type mismatch between values")
	}

	a := v.dfloat
	b := other.(*ValueFloat).dfloat
	switch {
	case a > b:
		return 1, nil
	case b > a:
		return -1, nil
	default:
		return 0, nil
	}
}

func (v *ValueFloat) Document() docs.Documentation {
	return docs.Text("Float")
}
