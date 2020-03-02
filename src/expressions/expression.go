// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package expressions

import (
	"datavalues"

	"base/docs"
)

type IExpression interface {
	Eval() error
	Result() datavalues.IDataValue
	Update(params IParams) (datavalues.IDataValue, error)
	Merge(arg IExpression) (datavalues.IDataValue, error)
	Walk(visit Visit) error
	String() string
	Document() docs.Documentation
}

type Visit func(e IExpression) (kontinue bool, err error)

func Walk(visit Visit, exprs ...IExpression) error {
	for _, expr := range exprs {
		if expr == nil {
			continue
		}
		kontinue, err := visit(expr)
		if err != nil {
			return err
		}
		if kontinue {
			err = expr.Walk(visit)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func expressionsFor(exprs ...interface{}) []IExpression {
	results := make([]IExpression, len(exprs))
	for i, expr := range exprs {
		results[i] = expressionFor(expr)
	}
	return results
}

func expressionFor(expr interface{}) IExpression {
	switch e := expr.(type) {
	case IExpression:
		return e
	case string:
		return VAR(e)
	case datavalues.IDataValue:
		return CONST(e)
	case int:
		return CONST(e)
	case int64:
		return CONST(e)
	case int32:
		return CONST(e)
	case int16:
		return CONST(e)
	case byte:
		return CONST(e)
	case float64:
		return CONST(e)
	case float32:
		return CONST(e)
	}
	return nil
}
