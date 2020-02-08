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
		update: func(current *datavalues.Value, next *datavalues.Value) *datavalues.Value {
			if current == nil {
				return next
			} else {
				return datavalues.ToValue(current.AsFloat() + next.AsFloat())
			}
		},
	}
}

func MIN(arg interface{}) IExpression {
	exprs := expressionsFor(arg)
	return &AggregateExpression{
		name: "MIN",
		expr: exprs[0],
		update: func(current *datavalues.Value, next *datavalues.Value) *datavalues.Value {
			if current == nil || current.AsFloat() > next.AsFloat() {
				return next
			}
			return current
		},
	}
}

func MAX(arg interface{}) IExpression {
	exprs := expressionsFor(arg)
	return &AggregateExpression{
		name: "MAX",
		expr: exprs[0],
		update: func(current *datavalues.Value, next *datavalues.Value) *datavalues.Value {
			if current == nil || current.AsFloat() < next.AsFloat() {
				return next
			} else {
				return current
			}
		},
	}
}

func COUNT(arg interface{}) IExpression {
	exprs := expressionsFor(arg)
	return &AggregateExpression{
		name: "COUNT",
		expr: exprs[0],
		update: func(current *datavalues.Value, next *datavalues.Value) *datavalues.Value {
			if current == nil {
				return datavalues.ToValue(1)
			} else {
				return datavalues.ToValue(current.AsFloat() + 1)
			}
		},
	}
}
