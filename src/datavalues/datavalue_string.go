// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package datavalues

import (
	"base/docs"
	"base/errors"
	"unsafe"
)

type ValueString struct {
	dstring string
}

func MakeString(v string) IDataValue {
	return &ValueString{dstring: v}
}

func ZeroString() IDataValue {
	return &ValueString{dstring: ""}
}

func (v *ValueString) Size() uintptr {
	return unsafe.Sizeof(*v) + uintptr(len(v.dstring))
}

func (v *ValueString) Show() []byte {
	return []byte(v.dstring)
}

func (v *ValueString) GetType() Type {
	return TypeString
}

func (v *ValueString) AsString() string {
	return v.dstring
}

func (v *ValueString) Compare(other IDataValue) (Comparison, error) {
	if other.GetType() != TypeString {
		return 0, errors.Errorf("type mismatch between values")
	}

	a := v.dstring
	b := other.(*ValueString).dstring
	switch {
	case a > b:
		return 1, nil
	case b > a:
		return -1, nil
	default:
		return 0, nil
	}
}

func (v *ValueString) Document() docs.Documentation {
	return docs.Text("String")
}
