// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package expressions

import (
	"datavalues"
)

type evalFunc func(left, right *datavalues.Value) (*datavalues.Value, error)

type BinaryExpression struct {
	name     string
	eval     evalFunc
	left     IExpression
	right    IExpression
	validate IValidator
}

func (e *BinaryExpression) Get() (*datavalues.Value, error) {
	var err error
	var left, right *datavalues.Value

	if left, err = e.left.Get(); err != nil {
		return nil, err
	}
	if right, err = e.right.Get(); err != nil {
		return nil, err
	}
	if e.validate != nil {
		if err := e.validate.Validate(left, right); err != nil {
			return nil, err
		}
	}
	return e.eval(left, right)
}

func (e *BinaryExpression) Update(params IParams) (*datavalues.Value, error) {
	var err error
	var left, right *datavalues.Value

	if left, err = e.left.Update(params); err != nil {
		return nil, err
	}
	if right, err = e.right.Update(params); err != nil {
		return nil, err
	}
	if e.validate != nil {
		if err := e.validate.Validate(left, right); err != nil {
			return nil, err
		}
	}
	return e.eval(left, right)
}

func (e *BinaryExpression) Walk(visit Visit) error {
	return Walk(visit, e.left, e.right)
}
