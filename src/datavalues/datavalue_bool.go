// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package datavalues

import (
	"unsafe"

	"base/docs"
	"base/errors"
)

type ValueBool bool

func MakeBool(v bool) IDataValue {
	r := ValueBool(v)
	return &r
}

func ZeroBool() IDataValue {
	r := ValueBool(false)
	return &r
}

func (v *ValueBool) Size() uintptr {
	return unsafe.Sizeof(*v)
}

func (v *ValueBool) GetType() Type {
	return TypeBool
}

func (v *ValueBool) AsBool() bool {
	return bool(*v)
}

func (v *ValueBool) Show() string {
	r := bool(*v)
	if r {
		return "true"
	} else {
		return "false"
	}
}

func (v *ValueBool) Compare(other IDataValue) (Comparison, error) {
	if other.GetType() != TypeBool {
		return 0, errors.Errorf("type mismatch between values")
	}

	a := bool(*v)
	b := AsBool(other)
	switch {
	case a == b:
		return 0, nil
	case a:
		return 1, nil
	default:
		return -1, nil
	}
}

func (v *ValueBool) Document() docs.Documentation {
	return docs.Text("Bool")
}

func AsBool(v IDataValue) bool {
	if t, ok := v.(*ValueBool); ok {
		return bool(*t)
	}
	return false
}
