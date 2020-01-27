// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package functions

import (
	"datatypes"
)

var FuncCompareEqual = &Function{
	Name: "=",
	Args: [][]string{
		{"left", "right"},
	},
	Logic: func(args ...datatypes.Value) (datatypes.Value, error) {
		v1 := args[0]
		v2 := args[1]
		cmp, err := datatypes.Compare(v1, v2)
		if err != nil || cmp != 0 {
			return datatypes.MakeBool(false), err
		}
		return datatypes.MakeBool(true), nil
	},
	Validator: All(
		ExactlyNArgs(2),
	),
}

var FuncCompareNotEqual = &Function{
	Name: "!=",
	Args: [][]string{
		{"left", "right"},
	},
	Logic: func(args ...datatypes.Value) (datatypes.Value, error) {
		v1 := args[0]
		v2 := args[1]
		cmp, err := datatypes.Compare(v1, v2)
		if err != nil || cmp == 0 {
			return datatypes.MakeBool(false), err
		}
		return datatypes.MakeBool(true), nil
	},
	Validator: All(
		ExactlyNArgs(2),
	),
}

var FuncCompareGreaterThan = &Function{
	Name: ">",
	Args: [][]string{
		{"left", "right"},
	},
	Logic: func(args ...datatypes.Value) (datatypes.Value, error) {
		v1 := args[0]
		v2 := args[1]
		cmp, err := datatypes.Compare(v1, v2)
		if err != nil || (cmp <= 0) {
			return datatypes.MakeBool(false), err
		}
		return datatypes.MakeBool(true), nil
	},
	Validator: All(
		ExactlyNArgs(2),
	),
}

var FuncCompareGreaterEqual = &Function{
	Name: ">=",
	Args: [][]string{
		{"left", "right"},
	},
	Logic: func(args ...datatypes.Value) (datatypes.Value, error) {
		v1 := args[0]
		v2 := args[1]
		cmp, err := datatypes.Compare(v1, v2)
		if err != nil || (cmp < 0) {
			return datatypes.MakeBool(false), err
		}
		return datatypes.MakeBool(true), nil
	},
	Validator: All(
		ExactlyNArgs(2),
	),
}

var FuncCompareLessThan = &Function{
	Name: "<",
	Args: [][]string{
		{"left", "right"},
	},
	Logic: func(args ...datatypes.Value) (datatypes.Value, error) {
		v1 := args[0]
		v2 := args[1]
		cmp, err := datatypes.Compare(v1, v2)
		if err != nil || (cmp >= 0) {
			return datatypes.MakeBool(false), err
		}
		return datatypes.MakeBool(true), nil
	},
	Validator: All(
		ExactlyNArgs(2),
	),
}

var FuncCompareLessEqual = &Function{
	Name: "<=",
	Args: [][]string{
		{"left", "right"},
	},
	Logic: func(args ...datatypes.Value) (datatypes.Value, error) {
		v1 := args[0]
		v2 := args[1]
		cmp, err := datatypes.Compare(v1, v2)
		if err != nil || (cmp > 0) {
			return datatypes.MakeBool(false), err
		}
		return datatypes.MakeBool(true), nil
	},
	Validator: All(
		ExactlyNArgs(2),
	),
}

var FuncCompareLike = &Function{
	Name: "LIKE",
	Args: [][]string{
		{"left", "right"},
	},
	Logic: func(args ...datatypes.Value) (datatypes.Value, error) {
		v1 := args[0]
		v2 := args[1]

		r := datatypes.Like(v2.ToRawValue().(string), v1)
		return datatypes.MakeBool(r), nil
	},
	Validator: All(
		ExactlyNArgs(2),
		OneOf(
			AllArgs(TypeOf(datatypes.ZeroString())),
		),
	),
}
