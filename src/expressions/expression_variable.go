// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package expressions

import (
	"datavalues"
)

type VariableExpression struct {
	value string
}

func VAR(v string) IExpression {
	return NewVariableExpression(v)
}

func NewVariableExpression(v string) *VariableExpression {
	return &VariableExpression{
		value: v,
	}
}

func (e *VariableExpression) Get() *datavalues.Value {
	return datavalues.ToValue(e.value)
}

func (e *VariableExpression) Update(params IParams) *datavalues.Value {
	v, _ := params.Get(e.value)
	return v
}

func (e *VariableExpression) Walk(visit Visit) error {
	return nil
}

func (e *VariableExpression) ReturnType() *datavalues.Value {
	return datavalues.ZeroString()
}
