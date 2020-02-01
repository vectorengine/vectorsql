// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package functions

import (
	"datavalues"
)

var FuncLogicAnd = &Function{
	Name: "AND",
	Args: [][]string{
		{"left", "right"},
	},
	Logic: func(args ...*datavalues.Value) (*datavalues.Value, error) {
		v1 := args[0]
		v2 := args[1]

		return datavalues.MakeBool(v1.AsBool() && v2.AsBool()), nil
	},
	Validator: All(
		ExactlyNArgs(2),
		OneOf(
			AllArgs(TypeOf(datavalues.ZeroBool())),
		),
	),
}

var FuncLogicOr = &Function{
	Name: "OR",
	Args: [][]string{
		{"left", "right"},
	},
	Logic: func(args ...*datavalues.Value) (*datavalues.Value, error) {
		v1 := args[0]
		v2 := args[1]

		return datavalues.MakeBool(v1.AsBool() || v2.AsBool()), nil
	},
	Validator: All(
		ExactlyNArgs(2),
		OneOf(
			AllArgs(TypeOf(datavalues.ZeroBool())),
		),
	),
}
