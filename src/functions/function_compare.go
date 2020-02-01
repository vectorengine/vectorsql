// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package functions

import (
	"datavalues"
)

var FuncCompareEqual = &Function{
	Name: "=",
	Args: [][]string{
		{"left", "right"},
	},
	Logic: func(args ...*datavalues.Value) (*datavalues.Value, error) {
		v1 := args[0]
		v2 := args[1]
		cmp, err := datavalues.Compare(v1, v2)
		if err != nil || cmp != 0 {
			return datavalues.MakeBool(false), err
		}
		return datavalues.MakeBool(true), nil
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
	Logic: func(args ...*datavalues.Value) (*datavalues.Value, error) {
		v1 := args[0]
		v2 := args[1]
		cmp, err := datavalues.Compare(v1, v2)
		if err != nil || cmp == 0 {
			return datavalues.MakeBool(false), err
		}
		return datavalues.MakeBool(true), nil
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
	Logic: func(args ...*datavalues.Value) (*datavalues.Value, error) {
		v1 := args[0]
		v2 := args[1]
		cmp, err := datavalues.Compare(v1, v2)
		if err != nil || (cmp <= 0) {
			return datavalues.MakeBool(false), err
		}
		return datavalues.MakeBool(true), nil
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
	Logic: func(args ...*datavalues.Value) (*datavalues.Value, error) {
		v1 := args[0]
		v2 := args[1]
		cmp, err := datavalues.Compare(v1, v2)
		if err != nil || (cmp < 0) {
			return datavalues.MakeBool(false), err
		}
		return datavalues.MakeBool(true), nil
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
	Logic: func(args ...*datavalues.Value) (*datavalues.Value, error) {
		v1 := args[0]
		v2 := args[1]
		cmp, err := datavalues.Compare(v1, v2)
		if err != nil || (cmp >= 0) {
			return datavalues.MakeBool(false), err
		}
		return datavalues.MakeBool(true), nil
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
	Logic: func(args ...*datavalues.Value) (*datavalues.Value, error) {
		v1 := args[0]
		v2 := args[1]
		cmp, err := datavalues.Compare(v1, v2)
		if err != nil || (cmp > 0) {
			return datavalues.MakeBool(false), err
		}
		return datavalues.MakeBool(true), nil
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
	Logic: func(args ...*datavalues.Value) (*datavalues.Value, error) {
		v1 := args[0]
		v2 := args[1]

		r := datavalues.Like(v2.ToRawValue().(string), v1)
		return datavalues.MakeBool(r), nil
	},
	Validator: All(
		ExactlyNArgs(2),
		OneOf(
			AllArgs(TypeOf(datavalues.ZeroString())),
		),
	),
}
