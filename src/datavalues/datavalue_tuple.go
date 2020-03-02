// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package datavalues

import (
	"strings"
	"unsafe"

	"base/docs"
)

type ValueTuple struct {
	fields []IDataValue
}

func MakeTuple(v ...IDataValue) IDataValue {
	return &ValueTuple{fields: v}
}

func ZeroTuple() IDataValue {
	return &ValueTuple{fields: nil}
}

func (v *ValueTuple) Size() uintptr {
	size := unsafe.Sizeof(*v)
	for _, field := range v.fields {
		size += field.Size()
	}
	return size
}

func (v *ValueTuple) String() string {
	result := make([]string, len(v.fields))
	for i := range v.fields {
		result[i] = v.fields[i].String()
	}
	return strings.Join(result, "")
}

func (v *ValueTuple) Type() Type {
	return TypeTuple
}

func (v *ValueTuple) Family() Family {
	return FamilyTuple
}

func (v *ValueTuple) AsSlice() []IDataValue {
	return v.fields
}

func (v *ValueTuple) Compare(other IDataValue) (Comparison, error) {
	otherv := other.(*ValueTuple)
	for i := range v.fields {
		cmp, err := v.fields[i].Compare(otherv.fields[i])
		if err != nil {
			return 0, err
		}
		switch cmp {
		case Equal:
			continue
		default:
			return cmp, nil
		}
	}
	return 0, nil
}

func (v *ValueTuple) Document() docs.Documentation {
	return docs.Text("Tuple")
}

func AsSlice(v IDataValue) []IDataValue {
	if t, ok := v.(*ValueTuple); ok {
		return t.fields
	}
	return nil
}
