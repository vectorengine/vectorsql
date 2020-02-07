// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package expressions

import (
	"datavalues"
)

func ADD(args ...interface{}) *Expression {
	exprs := expressionsFor(args...)
	return &Expression{
		name:  "+",
		exprs: exprs,
		eval: func(args ...*datavalues.Value) *datavalues.Value {
			left := args[0].AsInt()
			right := args[1].AsInt()
			return datavalues.ToValue(left + right)
		},
	}
}

func SUB(args ...interface{}) *Expression {
	exprs := expressionsFor(args...)
	return &Expression{
		name:  "-",
		exprs: exprs,
		eval: func(args ...*datavalues.Value) *datavalues.Value {
			left := args[0].AsInt()
			right := args[1].AsInt()
			return datavalues.ToValue(left - right)
		},
	}
}
