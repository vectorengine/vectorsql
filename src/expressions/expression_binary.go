// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package expressions

import (
	"datavalues"
)

type binaryEvalFunc func(left, right *datavalues.Value) (*datavalues.Value, error)

type BinaryExpression struct {
	name     string
	left     IExpression
	right    IExpression
	evalFn   binaryEvalFunc
	validate IValidator
}

func (e *BinaryExpression) Eval(params IParams) (*datavalues.Value, error) {
	var err error
	var left, right *datavalues.Value

	if left, err = e.left.Eval(params); err != nil {
		return nil, err
	}
	if right, err = e.right.Eval(params); err != nil {
		return nil, err
	}
	if e.validate != nil {
		if err := e.validate.Validate(left, right); err != nil {
			return nil, err
		}
	}
	return e.evalFn(left, right)
}

func (e *BinaryExpression) Walk(visit Visit) error {
	return Walk(visit, e.left, e.right)
}
