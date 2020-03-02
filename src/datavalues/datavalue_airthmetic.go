// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package datavalues

import (
	"base/errors"
)

func IsIntegral(v IDataValue) bool {
	typ := v.Type()
	return typ == TypeInt || typ == TypeInt32
}

func IsFloat(v IDataValue) bool {
	return v.Type() == TypeFloat
}

func IsNumber(v IDataValue) bool {
	return IsIntegral(v) || IsFloat(v)
}

func Add(v1 IDataValue, v2 IDataValue) (IDataValue, error) {
	if !IsNumber(v1) || !IsNumber(v2) {
		return nil, errors.Errorf("Unsupported type:(%v,%v)", v1.Type(), v2.Type())
	}

	switch v1.Type() {
	case TypeInt:
		v1 := AsInt(v1)
		v2 := AsInt(v2)
		return MakeInt(v1 + v2), nil
	case TypeInt32:
		v1 := AsInt(v1)
		v2 := AsInt(v2)
		return MakeInt32(int32(v1 + v2)), nil
	case TypeFloat:
		v1 := AsFloat(v1)
		v2 := AsFloat(v2)
		return MakeFloat(v1 + v2), nil
	}
	return nil, errors.Errorf("Unsupported type:(%v,%v)", v1.Type(), v2.Type())
}

func Sub(v1 IDataValue, v2 IDataValue) (IDataValue, error) {
	if !IsNumber(v1) || !IsNumber(v2) {
		return nil, errors.Errorf("Unsupported type:(%v,%v)", v1.Type(), v2.Type())
	}

	switch v1.Type() {
	case TypeInt:
		v1 := AsInt(v1)
		v2 := AsInt(v2)
		return MakeInt(v1 - v2), nil
	case TypeInt32:
		v1 := AsInt(v1)
		v2 := AsInt(v2)
		return MakeInt32(int32(v1 - v2)), nil
	case TypeFloat:
		v1 := AsFloat(v1)
		v2 := AsFloat(v2)
		return MakeFloat(v1 - v2), nil
	}
	return nil, errors.Errorf("Unsupported type:(%v,%v)", v1.Type(), v2.Type())
}

func Mul(v1 IDataValue, v2 IDataValue) (IDataValue, error) {
	if !IsNumber(v1) || !IsNumber(v2) {
		return nil, errors.Errorf("Unsupported type:(%v,%v)", v1.Type(), v2.Type())
	}

	switch v1.Type() {
	case TypeInt:
		v1 := AsInt(v1)
		v2 := AsInt(v2)
		return MakeInt(v1 * v2), nil
	case TypeInt32:
		v1 := AsInt(v1)
		v2 := AsInt(v2)
		return MakeInt32(int32(v1 * v2)), nil
	case TypeFloat:
		v1 := AsFloat(v1)
		v2 := AsFloat(v2)
		return MakeFloat(v1 * v2), nil
	}
	return nil, errors.Errorf("Unsupported type:(%v,%v)", v1.Type(), v2.Type())
}

func Div(v1 IDataValue, v2 IDataValue) (IDataValue, error) {
	if !IsNumber(v1) || !IsNumber(v2) {
		return nil, errors.Errorf("Unsupported type:(%v,%v)", v1.Type(), v2.Type())
	}

	switch v1.Type() {
	case TypeInt, TypeInt32:
		v1 := AsInt(v1)
		v2 := AsInt(v2)
		return MakeFloat(float64(v1) / float64(v2)), nil
	case TypeFloat:
		v1 := AsFloat(v1)
		v2 := AsFloat(v2)
		return MakeFloat(v1 / v2), nil
	}
	return nil, errors.Errorf("Unsupported type:(%v,%v)", v1.Type(), v2.Type())
}

func Min(v1 IDataValue, v2 IDataValue) (IDataValue, error) {
	cmp, err := v1.Compare(v2)
	if err != nil {
		return nil, err
	}
	if cmp == LessThan {
		return v1, nil
	} else {
		return v2, nil
	}
}

func Max(v1 IDataValue, v2 IDataValue) (IDataValue, error) {
	cmp, err := v1.Compare(v2)
	if err != nil {
		return nil, err
	}
	if cmp == GreaterThan {
		return v1, nil
	} else {
		return v2, nil
	}
}
