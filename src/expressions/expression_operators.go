// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package expressions

import (
	"datavalues"

	"base/errors"
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
			switch left.GetType() {
			case datavalues.TypeInt:
				return datavalues.ToValue(left.AsInt() + right.AsInt()), nil
			case datavalues.TypeFloat:
				return datavalues.ToValue(left.AsFloat() + right.AsFloat()), nil
			default:
				return nil, errors.Errorf("unsupported type:%+v", left)
			}
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
			switch left.GetType() {
			case datavalues.TypeInt:
				return datavalues.ToValue(left.AsInt() - right.AsInt()), nil
			case datavalues.TypeFloat:
				return datavalues.ToValue(left.AsFloat() - right.AsFloat()), nil
			default:
				return nil, errors.Errorf("unsupported type:%+v", left)
			}
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
			switch left.GetType() {
			case datavalues.TypeInt:
				return datavalues.ToValue(left.AsInt() * right.AsInt()), nil
			case datavalues.TypeFloat:
				return datavalues.ToValue(left.AsFloat() * right.AsFloat()), nil
			default:
				return nil, errors.Errorf("unsupported type:%+v", left)
			}
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
			switch left.GetType() {
			case datavalues.TypeInt:
				return datavalues.ToValue(left.AsInt() / right.AsInt()), nil
			case datavalues.TypeFloat:
				return datavalues.ToValue(left.AsFloat() / right.AsFloat()), nil
			default:
				return nil, errors.Errorf("unsupported type:%+v", left)
			}
		},
	}
}
