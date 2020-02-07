// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package expressions

import (
	"datavalues"
)

type evalFunc func(args ...*datavalues.Value) *datavalues.Value
type updateFunc func(current *datavalues.Value, next *datavalues.Value) *datavalues.Value

type IExpression interface {
	Get() *datavalues.Value
	Update(next IExpression) *datavalues.Value
}

type Expression struct {
	name   string
	eval   evalFunc
	update updateFunc
	exprs  []IExpression
	saved  *datavalues.Value
}

func (e *Expression) Get() *datavalues.Value {
	if e.update != nil {
		return e.saved
	}

	args := make([]*datavalues.Value, len(e.exprs))
	for i, expr := range e.exprs {
		args[i] = expr.Get()
	}
	return e.eval(args...)
}

func (e *Expression) Update(next IExpression) *datavalues.Value {
	if e.update != nil {
		if e.saved == nil {
			e.saved = next.Get()
		} else {
			e.saved = e.update(e.saved, next.Get())
		}
		return e.saved
	}
	return e.Get()
}

func expressionFor(expr interface{}) IExpression {
	switch e := expr.(type) {
	case IExpression:
		return e
	case *datavalues.Value:
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
