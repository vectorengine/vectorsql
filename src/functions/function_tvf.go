// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package functions

import (
	"fmt"
	"math/rand"

	"datatypes"
)

var FuncTableValuedFunctionRange = &Function{
	Name: "RANGE",
	Args: [][]string{
		{"left", "right"},
	},
	Logic: func(args ...*datatypes.Value) (*datatypes.Value, error) {
		v1 := args[0].AsInt()
		v2 := args[1].AsInt()
		values := make([]*datatypes.Value, v2-v1)

		for j, i := 0, v1; i < v2; j, i = j+1, i+1 {
			row := make([]*datatypes.Value, 1)
			row[0] = datatypes.MakeInt(i)
			values[j] = datatypes.MakeTuple(row...)
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

var FuncTableValuedFunctionRangeTable = &Function{
	Name: "RANGETABLE",
	Args: [][]string{
		{""},
	},
	Logic: func(args ...*datatypes.Value) (*datatypes.Value, error) {
		count := args[0].AsInt()
		values := make([]*datatypes.Value, count)
		for i := 0; i < count; i++ {
			row := make([]*datatypes.Value, len(args)-1)
			for j := 1; j < len(args); j++ {
				switch args[j].AsString() {
				case "String":
					row[j-1] = datatypes.MakeString(fmt.Sprintf("string-%v", i))
				case "UInt32", "Int32":
					row[j-1] = datatypes.MakeInt(i)
				}
			}
			values[i] = datatypes.MakeTuple(row...)
		}
		return datatypes.MakeTuple(values...), nil
	},
	Validator: All(
		AtLeastNArgs(2),
	),
}

var FuncTableValuedFunctionRandTable = &Function{
	Name: "RANDTABLE",
	Args: [][]string{
		{""},
	},
	Logic: func(args ...*datatypes.Value) (*datatypes.Value, error) {
		count := args[0].AsInt()
		values := make([]*datatypes.Value, count)
		for i := 0; i < count; i++ {
			row := make([]*datatypes.Value, len(args)-1)
			for j := 1; j < len(args); j++ {
				switch args[j].AsString() {
				case "String":
					row[j-1] = datatypes.MakeString(fmt.Sprintf("string-%v", rand.Intn(count)))
				case "UInt32", "Int32":
					row[j-1] = datatypes.MakeInt(rand.Intn(count))
				}
			}
			values[i] = datatypes.MakeTuple(row...)
		}
		return datatypes.MakeTuple(values...), nil
	},
	Validator: All(
		AtLeastNArgs(2),
	),
}

var FuncTableValuedFunctionZip = &Function{
	Name: "ZIP",
	Args: [][]string{
		{""},
	},
	Logic: func(args ...*datatypes.Value) (*datatypes.Value, error) {
		argsize := len(args)
		tuplesize := len(args[0].AsSlice())
		values := make([]*datatypes.Value, tuplesize)

		for i := 0; i < tuplesize; i++ {
			row := make([]*datatypes.Value, argsize)
			for j := 0; j < argsize; j++ {
				row[j] = args[j].AsSlice()[i]
			}
			values[i] = datatypes.MakeTuple(row...)
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
