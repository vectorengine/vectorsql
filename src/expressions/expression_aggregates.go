// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package expressions

import (
	"datavalues"
)

func SUM() *Expression {
	return &Expression{
		name: "SUM",
		update: func(current *datavalues.Value, next *datavalues.Value) *datavalues.Value {
			left := current.AsInt()
			right := next.AsInt()
			return datavalues.ToValue(left + right)
		},
	}
}

func MIN() *Expression {
	return &Expression{
		name: "MIN",
		update: func(current *datavalues.Value, next *datavalues.Value) *datavalues.Value {
			left := current.AsInt()
			right := next.AsInt()
			if left > right {
				return next
			} else {
				return current
			}
		},
	}
}

func MAX() *Expression {
	return &Expression{
		name: "MAX",
		update: func(current *datavalues.Value, next *datavalues.Value) *datavalues.Value {
			left := current.AsInt()
			right := next.AsInt()
			if left > right {
				return current
			} else {
				return next
			}
		},
	}
}
