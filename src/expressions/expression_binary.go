// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package expressions

import (
	"datavalues"
)

type evalFunc func(left, right *datavalues.Value) *datavalues.Value

type BinaryExpression struct {
	name  string
	eval  evalFunc
	left  IExpression
	right IExpression
}

func (e *BinaryExpression) Get() *datavalues.Value {
	left := e.left.Get()
	right := e.right.Get()
	return e.eval(left, right)
}

func (e *BinaryExpression) Update(params IParams) *datavalues.Value {
	left := e.left.Update(params)
	right := e.right.Update(params)
	return e.eval(left, right)
}
