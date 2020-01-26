// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package functions

import (
	"datatypes"
)

var FuncLogicAnd = &Function{
	Name: "AND",
	Args: [][]string{
		{"left", "right"},
	},
	Logic: func(args ...datatypes.Value) (datatypes.Value, error) {
		v1 := args[0]
		v2 := args[1]

		return datatypes.MakeBool(v1.AsBool() && v2.AsBool()), nil
	},
	Validator: All(
		ExactlyNArgs(2),
		OneOf(
			AllArgs(TypeOf(datatypes.ZeroBool())),
		),
	),
}

var FuncLogicOr = &Function{
	Name: "OR",
	Args: [][]string{
		{"left", "right"},
	},
	Logic: func(args ...datatypes.Value) (datatypes.Value, error) {
		v1 := args[0]
		v2 := args[1]

		return datatypes.MakeBool(v1.AsBool() || v2.AsBool()), nil
	},
	Validator: All(
		ExactlyNArgs(2),
		OneOf(
			AllArgs(TypeOf(datatypes.ZeroBool())),
		),
	),
}
