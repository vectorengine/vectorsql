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
	scalarExprCreator func(args ...interface{}) IExpression
)

var (
	unaryExprTable = map[string]unaryExprCreator{
		"SUM":   SUM,
		"MIN":   MIN,
		"MAX":   MAX,
		"COUNT": COUNT,
	}

	binaryExprTable = map[string]binaryExprCreator{
		"+":        ADD,
		"-":        SUB,
		"*":        MUL,
		"/":        DIV,
		">":        GT,
		">=":       GTE,
		"=":        EQ,
		"<":        LT,
		"<=":       LTE,
		"<>":       NEQ,
		"AND":      AND,
		"OR":       OR,
		"LIKE":     LIKE,
		"NOT LIKE": NOT_LIKE,
	}

	scalarExprTable = map[string]scalarExprCreator{
		"RANGETABLE": RANGETABLE,
		"RANDTABLE":  RANDTABLE,
		"ZIP":        ZIP,
		"IF":         IF,
	}
)

func ExpressionFactory(name string, args []interface{}) (IExpression, error) {
	name = strings.ToUpper(name)
	switch len(args) {
	case 1:
		if creator, ok := unaryExprTable[name]; ok {
			return creator(args[0]), nil
		}
	case 2:
		if creator, ok := binaryExprTable[name]; ok {
			return creator(args[0], args[1]), nil
		}
	}
	if creator, ok := scalarExprTable[name]; ok {
		return creator(args...), nil
	}
	return nil, errors.Errorf("Unsupported Expression:%v", name)
}
