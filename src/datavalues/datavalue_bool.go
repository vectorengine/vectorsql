// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package datavalues

import (
	"unsafe"

	"base/docs"
	"base/errors"
)

type ValueBool struct {
	dbool bool
}

func MakeBool(v bool) IDataValue {
	return &ValueBool{dbool: v}
}

func ZeroBool() IDataValue {
	return &ValueBool{dbool: false}
}

func (v *ValueBool) Size() uintptr {
	return unsafe.Sizeof(*v)
}

func (v *ValueBool) GetType() Type {
	return TypeBool
}

func (v *ValueBool) AsBool() bool {
	return v.dbool
}

func (v *ValueBool) Show() string {
	if v.dbool {
		return "true"
	} else {
		return "false"
	}
}

func (v *ValueBool) Compare(other IDataValue) (Comparison, error) {
	if other.GetType() != TypeBool {
		return 0, errors.Errorf("type mismatch between values")
	}

	a := v.dbool
	b := other.(*ValueBool).dbool
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
