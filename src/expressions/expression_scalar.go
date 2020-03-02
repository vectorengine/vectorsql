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

type scalarUpdateFunc func(args ...datavalues.IDataValue) (datavalues.IDataValue, error)
type ScalarExpression struct {
	name          string
	exprs         []IExpression
	saved         datavalues.IDataValue
	updateFn      scalarUpdateFunc
	validate      IValidator
	argumentNames [][]string
	description   docs.Documentation
}

func (e *ScalarExpression) Eval() error {
	var err error

	if e.saved == nil {
		values := make([]datavalues.IDataValue, len(e.exprs))

		for i, expr := range e.exprs {
			if err := expr.Eval(); err != nil {
				return err
			}
			values[i] = expr.Result()
		}
		if e.validate != nil {
			if err := e.validate.Validate(values...); err != nil {
				return err
			}
		}
		if e.saved, err = e.updateFn(values...); err != nil {
			return err
		}
	}
	return nil
}

func (e *ScalarExpression) Update(params IParams) (datavalues.IDataValue, error) {
	var err error
	values := make([]datavalues.IDataValue, len(e.exprs))

	for i, expr := range e.exprs {
		val, err := expr.Update(params)
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
	if e.saved, err = e.updateFn(values...); err != nil {
		return nil, err
	}
	return e.saved, nil
}

func (e *ScalarExpression) Merge(arg IExpression) (datavalues.IDataValue, error) {
	return e.saved, nil
}

func (e *ScalarExpression) Result() datavalues.IDataValue {
	return e.saved
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
