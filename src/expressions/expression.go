// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package expressions

import (
	"datavalues"
)

type IExpression interface {
	Get() *datavalues.Value
	Update(params IParams) *datavalues.Value
}

func expressionFor(expr interface{}) IExpression {
	switch e := expr.(type) {
	case IExpression:
		return e
	case string:
		return VAR(e)
	case *datavalues.Value:
		return CONST(e)
	case int:
		return CONST(float64(e))
	case int64:
		return CONST(float64(e))
	case int32:
		return CONST(float64(e))
	case int16:
		return CONST(float64(e))
	case byte:
		return CONST(float64(e))
	case float32:
		return CONST(float64(e))
	case float64:
		return CONST(e)
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
