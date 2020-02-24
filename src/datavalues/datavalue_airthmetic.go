// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package datavalues

import (
	"base/errors"
)

func IsIntegral(v IDataValue) bool {
	return v.GetType() == TypeInt
}

func IsFloat(v IDataValue) bool {
	return v.GetType() == TypeFloat
}

func IsNumber(v IDataValue) bool {
	return IsIntegral(v) || IsFloat(v)
}

func Add(v1 IDataValue, v2 IDataValue) (IDataValue, error) {
	if !IsNumber(v1) || !IsNumber(v2) {
		return nil, errors.Errorf("Unsupported type:(%v,%v)", v1.GetType(), v2.GetType())
	}

	switch v1.GetType() {
	case TypeInt:
		v1 := v1.(*ValueInt)
		v2 := v2.(*ValueInt)
		return MakeInt(v1.dint + v2.dint), nil
	case TypeFloat:
		v1 := v1.(*ValueFloat)
		v2 := v2.(*ValueFloat)
		return MakeFloat(v1.dfloat + v2.dfloat), nil
	}
	return nil, errors.Errorf("Unsupported type:(%v,%v)", v1.GetType(), v2.GetType())
}

func Sub(v1 IDataValue, v2 IDataValue) (IDataValue, error) {
	if !IsNumber(v1) || !IsNumber(v2) {
		return nil, errors.Errorf("Unsupported type:(%v,%v)", v1.GetType(), v2.GetType())
	}

	switch v1.GetType() {
	case TypeInt:
		v1 := v1.(*ValueInt)
		v2 := v2.(*ValueInt)
		return MakeInt(v1.dint - v2.dint), nil
	case TypeFloat:
		v1 := v1.(*ValueFloat)
		v2 := v2.(*ValueFloat)
		return MakeFloat(v1.dfloat - v2.dfloat), nil
	}
	return nil, errors.Errorf("Unsupported type:(%v,%v)", v1.GetType(), v2.GetType())
}

func Mul(v1 IDataValue, v2 IDataValue) (IDataValue, error) {
	if !IsNumber(v1) || !IsNumber(v2) {
		return nil, errors.Errorf("Unsupported type:(%v,%v)", v1.GetType(), v2.GetType())
	}

	switch v1.GetType() {
	case TypeInt:
		v1 := v1.(*ValueInt)
		v2 := v2.(*ValueInt)
		return MakeInt(v1.dint * v2.dint), nil
	case TypeFloat:
		v1 := v1.(*ValueFloat)
		v2 := v2.(*ValueFloat)
		return MakeFloat(v1.dfloat * v2.dfloat), nil
	}
	return nil, errors.Errorf("Unsupported type:(%v,%v)", v1.GetType(), v2.GetType())
}

func Div(v1 IDataValue, v2 IDataValue) (IDataValue, error) {
	if !IsNumber(v1) || !IsNumber(v2) {
		return nil, errors.Errorf("Unsupported type:(%v,%v)", v1.GetType(), v2.GetType())
	}

	switch v1.GetType() {
	case TypeInt:
		v1 := v1.(*ValueInt)
		v2 := v2.(*ValueInt)
		return MakeFloat(float64(v1.dint) / float64(v2.dint)), nil
	case TypeFloat:
		v1 := v1.(*ValueFloat)
		v2 := v2.(*ValueFloat)
		return MakeFloat(v1.dfloat / v2.dfloat), nil
	}
	return nil, errors.Errorf("Unsupported type:(%v,%v)", v1.GetType(), v2.GetType())
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
