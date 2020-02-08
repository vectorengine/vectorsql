// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package expressions

import (
	"datavalues"
)

type updateFunc func(current, next *datavalues.Value) *datavalues.Value

type AggregateExpression struct {
	name     string
	expr     IExpression
	update   updateFunc
	saved    *datavalues.Value
	exprtype *datavalues.Value
}

func (e *AggregateExpression) Get() *datavalues.Value {
	return e.saved
}

func (e *AggregateExpression) Update(params IParams) *datavalues.Value {
	updated := e.expr.Update(params)
	e.saved = e.update(e.saved, updated)
	return e.saved
}

func (e *AggregateExpression) Walk(visit Visit) error {
	return Walk(visit, e.expr)
}

func (e *AggregateExpression) ReturnType() *datavalues.Value {
	return e.exprtype
}
