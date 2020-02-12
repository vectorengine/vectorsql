// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package expressions

import (
	"fmt"
	"strings"

	"base/docs"
	"datavalues"
)

type scalarEvalFunc func(args ...*datavalues.Value) (*datavalues.Value, error)
type ScalarExpression struct {
	name          string
	exprs         []IExpression
	evalFn        scalarEvalFunc
	validate      IValidator
	argumentNames [][]string
	description   docs.Documentation
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

func (e *ScalarExpression) Document() docs.Documentation {
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
