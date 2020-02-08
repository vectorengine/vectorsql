// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package expressions

import (
	"math"

	"datavalues"
)

func ADD(left interface{}, right interface{}) IExpression {
	exprs := expressionsFor(left, right)
	return &BinaryExpression{
		name:     "+",
		left:     exprs[0],
		right:    exprs[1],
		exprtype: datavalues.ZeroFloat(),
		eval: func(left *datavalues.Value, right *datavalues.Value) *datavalues.Value {
			return datavalues.ToValue(left.AsFloat() + right.AsFloat())
		},
	}
}

func SUB(left interface{}, right interface{}) IExpression {
	exprs := expressionsFor(left, right)
	return &BinaryExpression{
		name:     "-",
		left:     exprs[0],
		right:    exprs[1],
		exprtype: datavalues.ZeroFloat(),
		eval: func(left *datavalues.Value, right *datavalues.Value) *datavalues.Value {
			return datavalues.ToValue(left.AsFloat() - right.AsFloat())
		},
	}
}

func MUL(left interface{}, right interface{}) IExpression {
	exprs := expressionsFor(left, right)
	return &BinaryExpression{
		name:     "*",
		left:     exprs[0],
		right:    exprs[1],
		exprtype: datavalues.ZeroFloat(),
		eval: func(left *datavalues.Value, right *datavalues.Value) *datavalues.Value {
			return datavalues.ToValue(left.AsFloat() * right.AsFloat())
		},
	}
}

func DIV(left interface{}, right interface{}) IExpression {
	exprs := expressionsFor(left, right)
	return &BinaryExpression{
		name:     "/",
		left:     exprs[0],
		right:    exprs[1],
		exprtype: datavalues.ZeroFloat(),
		eval: func(left *datavalues.Value, right *datavalues.Value) *datavalues.Value {
			if right == nil || right.AsFloat() == 0 {
				return datavalues.MakeFloat(math.MaxFloat64)
			}
			return datavalues.ToValue(left.AsFloat() / right.AsFloat())
		},
	}
}
