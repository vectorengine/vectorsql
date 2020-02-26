// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package datavalues

import (
	"fmt"
	"unsafe"

	"base/docs"
	"base/errors"
)

type ValueFloat float64

func MakeFloat(v float64) IDataValue {
	r := ValueFloat(v)
	return &r
}

func ZeroFloat() IDataValue {
	r := ValueFloat(0)
	return &r
}

func (v *ValueFloat) Size() uintptr {
	return unsafe.Sizeof(v)
}

func (v *ValueFloat) Show() string {
	return fmt.Sprintf("%v", *v)
}

func (v *ValueFloat) GetType() Type {
	return TypeFloat
}

func (v *ValueFloat) AsFloat() float64 {
	return float64(*v)
}

func (v *ValueFloat) Compare(other IDataValue) (Comparison, error) {
	if other.GetType() != TypeFloat {
		return 0, errors.Errorf("type mismatch between values")
	}

	a := float64(*v)
	b := AsFloat(other)
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

func AsFloat(v IDataValue) float64 {
	if t, ok := v.(*ValueFloat); ok {
		return float64(*t)
	}
	return 0.0
}
