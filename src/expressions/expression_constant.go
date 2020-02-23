// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package expressions

import (
	"base/docs"
	"datavalues"
)

type ConstantExpression struct {
	value datavalues.IDataValue
}

func CONST(v interface{}) IExpression {
	return NewConstantExpression(datavalues.ToValue(v))
}

func NewConstantExpression(v datavalues.IDataValue) *ConstantExpression {
	return &ConstantExpression{
		value: v,
	}
}

func (e *ConstantExpression) Result() (datavalues.IDataValue, error) {
	return e.value, nil
}

func (e *ConstantExpression) Update(params IParams) (datavalues.IDataValue, error) {
	return e.value, nil
}

func (e *ConstantExpression) Walk(visit Visit) error {
	return nil
}

func (e *ConstantExpression) String() string {
	return string(e.value.Show())
}

func (e *ConstantExpression) Document() docs.Documentation {
	return nil
}
