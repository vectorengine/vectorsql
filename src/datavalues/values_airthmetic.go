// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package datavalues

import (
	"base/errors"
)

func IsIntegral(v *Value) bool {
	return v.GetType() == TypeInt
}

func IsFloat(v *Value) bool {
	return v.GetType() == TypeFloat
}

func IsNumber(v *Value) bool {
	return IsIntegral(v) || IsFloat(v)
}

func Add(v1 *Value, v2 *Value) (*Value, error) {
	if !IsNumber(v1) || !IsNumber(v2) {
		return nil, errors.Errorf("Unsupported type:(%v,%v)", v1.GetType(), v2.GetType())
	}

	switch v1.GetType() {
	case TypeInt:
		return ToValue(v1.AsInt() + v2.AsInt()), nil
	case TypeFloat:
		return ToValue(v1.AsFloat() + v2.AsFloat()), nil
	}
	return nil, errors.Errorf("Unsupported type:(%v,%v)", v1.GetType(), v2.GetType())
}

func Sub(v1 *Value, v2 *Value) (*Value, error) {
	if !IsNumber(v1) || !IsNumber(v2) {
		return nil, errors.Errorf("Unsupported type:(%v,%v)", v1.GetType(), v2.GetType())
	}

	switch v1.GetType() {
	case TypeInt:
		return ToValue(v1.AsInt() - v2.AsInt()), nil
	case TypeFloat:
		return ToValue(v1.AsFloat() - v2.AsFloat()), nil
	}
	return nil, errors.Errorf("Unsupported type:(%v,%v)", v1.GetType(), v2.GetType())
}

func Mul(v1 *Value, v2 *Value) (*Value, error) {
	if !IsNumber(v1) || !IsNumber(v2) {
		return nil, errors.Errorf("Unsupported type:(%v,%v)", v1.GetType(), v2.GetType())
	}

	switch v1.GetType() {
	case TypeInt:
		return ToValue(v1.AsInt() * v2.AsInt()), nil
	case TypeFloat:
		return ToValue(v1.AsFloat() * v2.AsFloat()), nil
	}
	return nil, errors.Errorf("Unsupported type:(%v,%v)", v1.GetType(), v2.GetType())
}

func Div(v1 *Value, v2 *Value) (*Value, error) {
	if !IsNumber(v1) || !IsNumber(v2) {
		return nil, errors.Errorf("Unsupported type:(%v,%v)", v1.GetType(), v2.GetType())
	}

	switch v1.GetType() {
	case TypeInt:
		return ToValue(v1.AsInt() / v2.AsInt()), nil
	case TypeFloat:
		return ToValue(v1.AsFloat() / v2.AsFloat()), nil
	}
	return nil, errors.Errorf("Unsupported type:(%v,%v)", v1.GetType(), v2.GetType())
}

func Min(v1 *Value, v2 *Value) (*Value, error) {
	cmp, err := Compare(v1, v2)
	if err != nil {
		return nil, err
	}
	if cmp == LessThan {
		return v1, nil
	} else {
		return v2, nil
	}
}

func Max(v1 *Value, v2 *Value) (*Value, error) {
	cmp, err := Compare(v1, v2)
	if err != nil {
		return nil, err
	}
	if cmp == GreaterThan {
		return v1, nil
	} else {
		return v2, nil
	}
}
