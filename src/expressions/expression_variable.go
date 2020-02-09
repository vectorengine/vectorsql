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

func (e *VariableExpression) Eval(params IParams) (*datavalues.Value, error) {
	if params != nil {
		v, _ := params.Get(e.value)
		return v, nil
	}
	return datavalues.MakePhantom(), nil
}

func (e *VariableExpression) Walk(visit Visit) error {
	return nil
}
