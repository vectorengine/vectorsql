// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package expressions

import (
	"fmt"
	"strings"

	"datavalues"

	"base/docs"
)

type binaryEvalFunc func(left, right *datavalues.Value) (*datavalues.Value, error)

type BinaryExpression struct {
	name          string
	left          IExpression
	right         IExpression
	evalFn        binaryEvalFunc
	validate      IValidator
	argumentNames [][]string
	description   docs.Documentation
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

func (e *BinaryExpression) String() string {
	return fmt.Sprintf("(%v%v%v)", e.left, e.name, e.right)
}

func (e *BinaryExpression) Document() docs.Documentation {
	callingWays := make([]docs.Documentation, len(e.argumentNames))
	for i, arguments := range e.argumentNames {
		callingWays[i] = docs.Text(fmt.Sprintf("%s(%s)", e.name, strings.Join(arguments, ", ")))
	}
	return docs.Section(
		e.name,
		docs.Body(
			docs.Section("Calling", docs.List(callingWays...)),
			docs.Section("Arguments", e.validate.Document()),
			docs.Section("Description", e.description),
		),
	)
}
