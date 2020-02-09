// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package expressions

import (
	"strings"

	"base/errors"
)

type (
	unaryExprCreator  func(arg interface{}) IExpression
	binaryExprCreator func(left interface{}, right interface{}) IExpression
)

var (
	unaryExprTable = map[string]unaryExprCreator{
		"SUM":   SUM,
		"MIN":   MIN,
		"MAX":   MAX,
		"COUNT": COUNT,
	}

	binaryExprTable = map[string]binaryExprCreator{
		"+":    ADD,
		"-":    SUB,
		"*":    MUL,
		"/":    DIV,
		">":    GT,
		">=":   GTE,
		"=":    EQ,
		"<":    LT,
		"<=":   LTE,
		"<>":   NEQ,
		"AND":  AND,
		"OR":   OR,
		"LIKE": LIKE,
	}
)

func UnaryExpressionFactory(name string, arg interface{}) (IExpression, error) {
	name = strings.ToUpper(name)
	fn, ok := unaryExprTable[name]
	if !ok {
		return nil, errors.Errorf("Unsupported Binary Expression:%v", name)
	}
	return fn(arg), nil
}

func BinaryExpressionFactory(name string, left interface{}, right interface{}) (IExpression, error) {
	name = strings.ToUpper(name)
	fn, ok := binaryExprTable[name]
	if !ok {
		return nil, errors.Errorf("Unsupported Binary Expression:%v", name)
	}
	return fn(left, right), nil
}
