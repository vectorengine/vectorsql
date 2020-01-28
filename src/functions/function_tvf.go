// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package functions

import (
	"datatypes"
)

var FuncTableValuedFunctionRange = &Function{
	Name: "RANGE",
	Args: [][]string{
		{"left", "right"},
	},
	Logic: func(args ...datatypes.Value) (datatypes.Value, error) {
		v1 := args[0].AsInt()
		v2 := args[1].AsInt()
		values := make([]datatypes.Value, v2-v1)

		for j, i := 0, v1; i < v2; j, i = j+1, i+1 {
			values[j] = datatypes.MakeInt(i)
		}
		return datatypes.MakeTuple(values), nil
	},
	Validator: All(
		ExactlyNArgs(2),
		OneOf(
			AllArgs(TypeOf(datatypes.ZeroInt())),
		),
	),
}
