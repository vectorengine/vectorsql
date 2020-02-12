// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package expressions

import (
	"base/docs"
	"datavalues"
	"fmt"
	"strings"
)

type aggregateEvalFunc func(current, next *datavalues.Value) (*datavalues.Value, error)

type AggregateExpression struct {
	name          string
	expr          IExpression
	evalFn        aggregateEvalFunc
	saved         *datavalues.Value
	validate      IValidator
	argumentNames [][]string
	description   docs.Documentation
}

func (e *AggregateExpression) Eval(params IParams) (*datavalues.Value, error) {
	var err error
	var updated *datavalues.Value

	if updated, err = e.expr.Eval(params); err != nil {
		return nil, err
	}
	if e.validate != nil {
		if err := e.validate.Validate(updated); err != nil {
			return nil, err
		}
	}
	if e.saved, err = e.evalFn(e.saved, updated); err != nil {
		return nil, err
	}
	return e.saved, nil
}

func (e *AggregateExpression) Walk(visit Visit) error {
	return Walk(visit, e.expr)
}

func (e *AggregateExpression) String() string {
	return fmt.Sprintf("%v(%v)", e.name, e.expr)
}

func (e *AggregateExpression) Document() docs.Documentation {
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
