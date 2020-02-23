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

type aggregateUpdateFunc func(current, next datavalues.IDataValue) (datavalues.IDataValue, error)

type AggregateExpression struct {
	name          string
	expr          IExpression
	updateFn      aggregateUpdateFunc
	saved         datavalues.IDataValue
	validate      IValidator
	argumentNames [][]string
	description   docs.Documentation
}

func (e *AggregateExpression) Get() (datavalues.IDataValue, error) {
	return e.saved, nil
}

func (e *AggregateExpression) Update(params IParams) (datavalues.IDataValue, error) {
	var err error
	var updated datavalues.IDataValue

	if updated, err = e.expr.Update(params); err != nil {
		return nil, err
	}
	if e.validate != nil {
		if err := e.validate.Validate(updated); err != nil {
			return nil, err
		}
	}
	if e.saved, err = e.updateFn(e.saved, updated); err != nil {
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
