// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package expressions

import (
	"fmt"

	"base/docs"
	"datavalues"
)

type ConstantExpression struct {
	value *datavalues.Value
}

func CONST(v interface{}) IExpression {
	return NewConstantExpression(datavalues.ToValue(v))
}

func NewConstantExpression(v *datavalues.Value) *ConstantExpression {
	return &ConstantExpression{
		value: v,
	}
}

func (e *ConstantExpression) Get() (*datavalues.Value, error) {
	return e.value, nil
}

func (e *ConstantExpression) Update(params IParams) (*datavalues.Value, error) {
	return e.value, nil
}

func (e *ConstantExpression) Walk(visit Visit) error {
	return nil
}

func (e *ConstantExpression) String() string {
	return fmt.Sprintf("%v", e.value)
}

func (e *ConstantExpression) Document() docs.Documentation {
	return nil
}
