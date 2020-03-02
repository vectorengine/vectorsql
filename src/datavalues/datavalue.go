// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package datavalues

import (
	"fmt"

	"base/docs"
)

type Type int

const (
	TypeZero Type = iota
	TypeNull
	TypePhantom
	TypeInt
	TypeInt32
	TypeFloat
	TypeBool
	TypeString
	TypeTime
	TypeDuration
	TypeTuple
	TypeObject
)

type Comparison int

const (
	LessThan    Comparison = -1
	Equal       Comparison = 0
	GreaterThan Comparison = 1
)

type Family int32

const (
	FamilyBool Family = iota
	FamilyInt
	FamilyFloat
	FamilyString
	FamilyTuple
)

type IDataValue interface {
	Size() uintptr
	Type() Type
	Family() Family
	String() string
	Compare(value IDataValue) (Comparison, error)
	Document() docs.Documentation
}

// NormalizeType brings various primitive types into the type we want them to be.
// All types coming out of data sources have to be already normalized this way.
func ToValue(value interface{}) IDataValue {
	switch value := value.(type) {
	case bool:
		return MakeBool(value)
	case int:
		return MakeInt32(int32(value))
	case int8:
		return MakeInt32(int32(value))
	case int16:
		return MakeInt32(int32(value))
	case int32:
		return MakeInt32(value)
	case int64:
		return MakeInt(int64(value))
	case uint8:
		return MakeInt(int64(value))
	case uint32:
		return MakeInt(int64(value))
	case uint64:
		return MakeInt(int64(value))
	case float32:
		return MakeFloat(float64(value))
	case float64:
		return MakeFloat(value)
	case []byte:
		return MakeString(string(value))
	case string:
		return MakeString(value)
	case []interface{}:
		out := make([]IDataValue, len(value))
		for i := range value {
			out[i] = ToValue(value[i])
		}
		return MakeTuple(out...)
	case IDataValue:
		return value
	}
	panic(fmt.Sprintf("unreachable:%T", value))
}
