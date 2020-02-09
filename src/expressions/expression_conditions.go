// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package expressions

import (
	"datavalues"
)

func LT(left interface{}, right interface{}) IExpression {
	exprs := expressionsFor(left, right)
	return &BinaryExpression{
		name:     "<",
		left:     exprs[0],
		right:    exprs[1],
		validate: All(),
		eval: func(left *datavalues.Value, right *datavalues.Value) (*datavalues.Value, error) {
			cmp, err := datavalues.Compare(left, right)
			if err != nil {
				return nil, err
			}
			return datavalues.MakeBool(cmp == datavalues.LessThan), nil
		},
	}
}

func LTE(left interface{}, right interface{}) IExpression {
	exprs := expressionsFor(left, right)
	return &BinaryExpression{
		name:     "<=",
		left:     exprs[0],
		right:    exprs[1],
		validate: All(),
		eval: func(left *datavalues.Value, right *datavalues.Value) (*datavalues.Value, error) {
			cmp, err := datavalues.Compare(left, right)
			if err != nil {
				return nil, err
			}
			return datavalues.MakeBool(cmp < datavalues.GreaterThan), nil
		},
	}
}

func EQ(left interface{}, right interface{}) IExpression {
	exprs := expressionsFor(left, right)
	return &BinaryExpression{
		name:     "=",
		left:     exprs[0],
		right:    exprs[1],
		validate: All(),
		eval: func(left *datavalues.Value, right *datavalues.Value) (*datavalues.Value, error) {
			cmp, err := datavalues.Compare(left, right)
			if err != nil {
				return nil, err
			}
			return datavalues.MakeBool(cmp == datavalues.Equal), nil
		},
	}
}

func NEQ(left interface{}, right interface{}) IExpression {
	exprs := expressionsFor(left, right)
	return &BinaryExpression{
		name:     "<>",
		left:     exprs[0],
		right:    exprs[1],
		validate: All(),
		eval: func(left *datavalues.Value, right *datavalues.Value) (*datavalues.Value, error) {
			cmp, err := datavalues.Compare(left, right)
			if err != nil {
				return nil, err
			}
			return datavalues.MakeBool(cmp != datavalues.Equal), nil
		},
	}
}

func GT(left interface{}, right interface{}) IExpression {
	exprs := expressionsFor(left, right)
	return &BinaryExpression{
		name:     ">",
		left:     exprs[0],
		right:    exprs[1],
		validate: All(),
		eval: func(left *datavalues.Value, right *datavalues.Value) (*datavalues.Value, error) {
			cmp, err := datavalues.Compare(left, right)
			if err != nil {
				return nil, err
			}
			return datavalues.MakeBool(cmp == datavalues.GreaterThan), nil
		},
	}
}

func GTE(left interface{}, right interface{}) IExpression {
	exprs := expressionsFor(left, right)
	return &BinaryExpression{
		name:     ">=",
		left:     exprs[0],
		right:    exprs[1],
		validate: All(),
		eval: func(left *datavalues.Value, right *datavalues.Value) (*datavalues.Value, error) {
			cmp, err := datavalues.Compare(left, right)
			if err != nil {
				return nil, err
			}
			return datavalues.MakeBool(cmp > datavalues.LessThan), nil
		},
	}
}

func AND(left interface{}, right interface{}) IExpression {
	exprs := expressionsFor(left, right)
	return &BinaryExpression{
		name:     "AND",
		left:     exprs[0],
		right:    exprs[1],
		validate: All(),
		eval: func(left *datavalues.Value, right *datavalues.Value) (*datavalues.Value, error) {
			return datavalues.ToValue(left.AsBool() && right.AsBool()), nil
		},
	}
}

func OR(left interface{}, right interface{}) IExpression {
	exprs := expressionsFor(left, right)
	return &BinaryExpression{
		name:     "OR",
		left:     exprs[0],
		right:    exprs[1],
		validate: All(),
		eval: func(left *datavalues.Value, right *datavalues.Value) (*datavalues.Value, error) {
			return datavalues.ToValue(left.AsBool() || right.AsBool()), nil
		},
	}
}

func LIKE(left interface{}, right interface{}) IExpression {
	exprs := expressionsFor(left, right)
	return &BinaryExpression{
		name:     "LIKE",
		left:     exprs[0],
		right:    exprs[1],
		validate: All(),
		eval: func(left *datavalues.Value, right *datavalues.Value) (*datavalues.Value, error) {
			return datavalues.ToValue(datavalues.Like(right.AsString(), left)), nil
		},
	}
}
