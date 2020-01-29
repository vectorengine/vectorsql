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
		return datatypes.MakeTuple(values...), nil
	},
	Validator: All(
		ExactlyNArgs(2),
		OneOf(
			AllArgs(TypeOf(datatypes.ZeroInt())),
		),
	),
}

var FuncTableValuedFunctionZip = &Function{
	Name: "ZIP",
	Args: [][]string{
		{""},
	},
	Logic: func(args ...datatypes.Value) (datatypes.Value, error) {
		argsize := len(args)
		tuplesize := len(args[0].AsSlice())
		values := make([]datatypes.Value, tuplesize)

		for i := 0; i < tuplesize; i++ {
			var v []datatypes.Value
			for j := 0; j < argsize; j++ {
				v = append(v, args[j].AsSlice()[i])
			}
			values[i] = datatypes.MakeTuple(v...)
		}
		return datatypes.MakeTuple(values...), nil
	},
	Validator: All(
		AtLeastNArgs(2),
		OneOf(
			AllArgs(TypeOf(datatypes.ZeroTuple())),
		),
	),
}
