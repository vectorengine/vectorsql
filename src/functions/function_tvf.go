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
		var values []datatypes.Value

		v1 := args[0]
		v2 := args[1]

		for i := v1.AsInt(); i < v2.AsInt(); i++ {
			values = append(values, datatypes.MakeInt(i))
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
