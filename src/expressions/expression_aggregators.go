// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package expressions

import (
	"datavalues"
)

func SUM(arg interface{}) IExpression {
	exprs := expressionsFor(arg)
	return &AggregateExpression{
		name: "SUM",
		expr: exprs[0],
		validate: All(
			OneOf(
				AllArgs(TypeOf(datavalues.ZeroInt())),
				AllArgs(TypeOf(datavalues.ZeroFloat())),
			),
		),
		update: func(current *datavalues.Value, next *datavalues.Value) (*datavalues.Value, error) {
			if current == nil {
				return next, nil
			} else {
				return datavalues.Add(current, next)
			}
		},
	}
}

func MIN(arg interface{}) IExpression {
	exprs := expressionsFor(arg)
	return &AggregateExpression{
		name: "MIN",
		expr: exprs[0],
		validate: All(
			OneOf(
				AllArgs(TypeOf(datavalues.ZeroInt())),
				AllArgs(TypeOf(datavalues.ZeroFloat())),
			),
		),
		update: func(current *datavalues.Value, next *datavalues.Value) (*datavalues.Value, error) {
			if current == nil {
				return next, nil
			}
			return datavalues.Min(current, next)
		},
	}
}

func MAX(arg interface{}) IExpression {
	exprs := expressionsFor(arg)
	return &AggregateExpression{
		name: "MAX",
		expr: exprs[0],
		validate: All(
			OneOf(
				AllArgs(TypeOf(datavalues.ZeroInt())),
				AllArgs(TypeOf(datavalues.ZeroFloat())),
			),
		),
		update: func(current *datavalues.Value, next *datavalues.Value) (*datavalues.Value, error) {
			if current == nil {
				return next, nil
			}
			return datavalues.Max(current, next)
		},
	}
}

func COUNT(arg interface{}) IExpression {
	exprs := expressionsFor(arg)
	return &AggregateExpression{
		name: "COUNT",
		expr: exprs[0],
		update: func(current *datavalues.Value, next *datavalues.Value) (*datavalues.Value, error) {
			if current == nil {
				return datavalues.ToValue(1), nil
			} else {
				return datavalues.Add(current, datavalues.ToValue(1))
			}
		},
	}
}
