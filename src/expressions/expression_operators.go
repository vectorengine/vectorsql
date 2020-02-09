// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package expressions

import (
	"datavalues"
)

func ADD(left interface{}, right interface{}) IExpression {
	exprs := expressionsFor(left, right)
	return &BinaryExpression{
		name:  "+",
		left:  exprs[0],
		right: exprs[1],
		validate: All(
			OneOf(
				AllArgs(TypeOf(datavalues.ZeroInt())),
				AllArgs(TypeOf(datavalues.ZeroFloat())),
			),
		),
		eval: func(left *datavalues.Value, right *datavalues.Value) (*datavalues.Value, error) {
			return datavalues.Add(left, right)
		},
	}
}

func SUB(left interface{}, right interface{}) IExpression {
	exprs := expressionsFor(left, right)
	return &BinaryExpression{
		name:  "-",
		left:  exprs[0],
		right: exprs[1],
		validate: All(
			OneOf(
				AllArgs(TypeOf(datavalues.ZeroInt())),
				AllArgs(TypeOf(datavalues.ZeroFloat())),
			),
		),
		eval: func(left *datavalues.Value, right *datavalues.Value) (*datavalues.Value, error) {
			return datavalues.Sub(left, right)
		},
	}
}

func MUL(left interface{}, right interface{}) IExpression {
	exprs := expressionsFor(left, right)
	return &BinaryExpression{
		name:  "*",
		left:  exprs[0],
		right: exprs[1],
		validate: All(
			OneOf(
				AllArgs(TypeOf(datavalues.ZeroInt())),
				AllArgs(TypeOf(datavalues.ZeroFloat())),
			),
		),
		eval: func(left *datavalues.Value, right *datavalues.Value) (*datavalues.Value, error) {
			return datavalues.Mul(left, right)
		},
	}
}

func DIV(left interface{}, right interface{}) IExpression {
	exprs := expressionsFor(left, right)
	return &BinaryExpression{
		name:  "/",
		left:  exprs[0],
		right: exprs[1],
		validate: All(
			OneOf(
				AllArgs(TypeOf(datavalues.ZeroInt())),
				AllArgs(TypeOf(datavalues.ZeroFloat())),
			),
		),
		eval: func(left *datavalues.Value, right *datavalues.Value) (*datavalues.Value, error) {
			return datavalues.Div(left, right)
		},
	}
}
