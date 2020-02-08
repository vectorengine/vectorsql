// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package expressions

import (
	"base/errors"
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
				switch current.GetType() {
				case datavalues.TypeInt:
					return datavalues.ToValue(current.AsInt() + next.AsInt()), nil
				case datavalues.TypeFloat:
					return datavalues.ToValue(current.AsFloat() + next.AsFloat()), nil
				default:
					return nil, errors.Errorf("unsupported type:%+v", current)
				}
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
			cmp, err := datavalues.Compare(current, next)
			if err != nil {
				return nil, err
			}
			if cmp == datavalues.GreaterThan {
				return next, nil
			} else {
				return current, nil
			}
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
			cmp, err := datavalues.Compare(current, next)
			if err != nil {
				return nil, err
			}
			if cmp == datavalues.LessThan {
				return next, nil
			} else {
				return current, nil
			}
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
				return datavalues.ToValue(current.AsFloat() + 1), nil
			}
		},
	}
}
