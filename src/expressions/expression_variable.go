// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package expressions

import (
	"base/docs"
	"base/errors"
	"datavalues"
)

type VariableExpression struct {
	value string
	saved datavalues.IDataValue
}

func VAR(v string) IExpression {
	return NewVariableExpression(v)
}

func NewVariableExpression(v string) *VariableExpression {
	return &VariableExpression{
		value: v,
	}
}

func (e *VariableExpression) Result() (datavalues.IDataValue, error) {
	return e.saved, nil
}

func (e *VariableExpression) Update(params IParams) (datavalues.IDataValue, error) {
	if params != nil {
		v, ok := params.Get(e.value)
		if !ok {
			return nil, errors.Errorf("Can't get the params:%v value", e.value)
		}
		e.saved = v
		return v, nil
	}
	return nil, nil
}

func (e *VariableExpression) Merge(arg IExpression) (datavalues.IDataValue, error) {
	other := arg.(*VariableExpression)
	return other.saved, nil
}

func (e *VariableExpression) Walk(visit Visit) error {
	return nil
}

func (e *VariableExpression) String() string {
	return e.value
}

func (e *VariableExpression) Document() docs.Documentation {
	return nil
}
