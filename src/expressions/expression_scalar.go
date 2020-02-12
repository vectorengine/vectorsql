// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package expressions

import (
	"datavalues"
	"fmt"
)

type scalarEvalFunc func(args ...*datavalues.Value) (*datavalues.Value, error)
type ScalarExpression struct {
	name     string
	exprs    []IExpression
	evalFn   scalarEvalFunc
	validate IValidator
}

func (e *ScalarExpression) Eval(params IParams) (*datavalues.Value, error) {
	values := make([]*datavalues.Value, len(e.exprs))

	for i, expr := range e.exprs {
		val, err := expr.Eval(params)
		if err != nil {
			return nil, err
		}
		values[i] = val
	}
	if e.validate != nil {
		if err := e.validate.Validate(values...); err != nil {
			return nil, err
		}
	}
	return e.evalFn(values...)
}

func (e *ScalarExpression) Walk(visit Visit) error {
	return Walk(visit, e.exprs...)
}

func (e *ScalarExpression) String() string {
	return fmt.Sprintf("%v(%v)", e.name, e.exprs)
}
