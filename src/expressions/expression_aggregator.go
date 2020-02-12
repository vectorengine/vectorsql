// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package expressions

import (
	"datavalues"

	"base/docs"
)

func SUM(arg interface{}) IExpression {
	return &AggregateExpression{
		name:          "SUM",
		argumentNames: [][]string{},
		description:   docs.Text("Sums Floats, Ints or Durations in the group. You may not mix types."),
		validate: All(
			OneOf(
				AllArgs(TypeOf(datavalues.ZeroInt())),
				AllArgs(TypeOf(datavalues.ZeroFloat())),
			),
		),
		expr: expressionsFor(arg)[0],
		evalFn: func(current *datavalues.Value, next *datavalues.Value) (*datavalues.Value, error) {
			if current == nil {
				return next, nil
			} else {
				return datavalues.Add(current, next)
			}
		},
	}
}

func MIN(arg interface{}) IExpression {
	return &AggregateExpression{
		name:          "MIN",
		argumentNames: [][]string{},
		description:   docs.Text("Takes the minimum element in the group. Works with Ints, Floats, Strings, Booleans, Times, Durations."),
		validate: All(
			OneOf(
				AllArgs(TypeOf(datavalues.ZeroInt())),
				AllArgs(TypeOf(datavalues.ZeroFloat())),
			),
		),
		expr: expressionsFor(arg)[0],
		evalFn: func(current *datavalues.Value, next *datavalues.Value) (*datavalues.Value, error) {
			if current == nil {
				return next, nil
			}
			return datavalues.Min(current, next)
		},
	}
}

func MAX(arg interface{}) IExpression {
	return &AggregateExpression{
		name:          "MAX",
		argumentNames: [][]string{},
		description:   docs.Text("Takes the maximum element in the group. Works with Ints, Floats, Strings, Booleans, Times, Durations."),
		validate: All(
			OneOf(
				AllArgs(TypeOf(datavalues.ZeroInt())),
				AllArgs(TypeOf(datavalues.ZeroFloat())),
			),
		),
		expr: expressionsFor(arg)[0],
		evalFn: func(current *datavalues.Value, next *datavalues.Value) (*datavalues.Value, error) {
			if current == nil {
				return next, nil
			}
			return datavalues.Max(current, next)
		},
	}
}

func COUNT(arg interface{}) IExpression {
	return &AggregateExpression{
		name:          "COUNT",
		argumentNames: [][]string{},
		description:   docs.Text("Averages elements in the group."),
		validate:      All(),
		expr:          expressionsFor(arg)[0],
		evalFn: func(current *datavalues.Value, next *datavalues.Value) (*datavalues.Value, error) {
			if current == nil {
				return datavalues.ToValue(1), nil
			} else {
				return datavalues.Add(current, datavalues.ToValue(1))
			}
		},
	}
}
