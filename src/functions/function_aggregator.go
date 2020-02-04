// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package functions

import (
	"datavalues"
)

var FuncAggregatorMax = &Function{
	Name: "MAX",
	Args: [][]string{
		{},
	},
	Logic: func(args ...*datavalues.Value) (*datavalues.Value, error) {
		var max *datavalues.Value
		for _, arg := range args {
			if max == nil {
				max = arg
			} else {
				cmp, err := datavalues.Compare(arg, max)
				if err != nil {
					return nil, err
				}
				if cmp == datavalues.GreaterThan {
					max = arg
				}
			}
		}
		return max, nil
	},
	Validator: All(
		AtLeastNArgs(1),
		All(
			AllArgs(TypeOf(datavalues.ZeroInt())),
		),
	),
}

var FuncAggregatorMin = &Function{
	Name: "MIN",
	Args: [][]string{
		{},
	},
	Logic: func(args ...*datavalues.Value) (*datavalues.Value, error) {
		var max *datavalues.Value
		for _, arg := range args {
			if max == nil {
				max = arg
			} else {
				cmp, err := datavalues.Compare(arg, max)
				if err != nil {
					return nil, err
				}
				if cmp == datavalues.LessThan {
					max = arg
				}
			}
		}
		return max, nil
	},
	Validator: All(
		AtLeastNArgs(1),
		All(
			AllArgs(TypeOf(datavalues.ZeroInt())),
		),
	),
}

var FuncAggregatorCount = &Function{
	Name: "COUNT",
	Args: [][]string{
		{},
	},
	Logic: func(args ...*datavalues.Value) (*datavalues.Value, error) {
		return datavalues.ToValue(len(args)), nil
	},
	Validator: All(
		All(),
	),
}
