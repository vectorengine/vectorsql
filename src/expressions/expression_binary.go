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

type binaryUpdateFunc func(left, right datavalues.IDataValue) (datavalues.IDataValue, error)

type BinaryExpression struct {
	name          string
	left          IExpression
	right         IExpression
	saved         datavalues.IDataValue
	updateFn      binaryUpdateFunc
	validate      IValidator
	argumentNames [][]string
	description   docs.Documentation
}

func (e *BinaryExpression) Result() (datavalues.IDataValue, error) {
	var err error
	var left, right datavalues.IDataValue

	if e.saved == nil {
		if left, err = e.left.Result(); err != nil {
			return nil, err
		}
		if right, err = e.right.Result(); err != nil {
			return nil, err
		}
		if e.validate != nil {
			if err := e.validate.Validate(left, right); err != nil {
				return nil, err
			}
		}
		if e.saved, err = e.updateFn(left, right); err != nil {
			return nil, err
		}
	}
	return e.saved, nil
}

func (e *BinaryExpression) Update(params IParams) (datavalues.IDataValue, error) {
	var err error
	var left, right datavalues.IDataValue

	if left, err = e.left.Update(params); err != nil {
		return nil, err
	}
	if right, err = e.right.Update(params); err != nil {
		return nil, err
	}
	if e.validate != nil {
		if err := e.validate.Validate(left, right); err != nil {
			return nil, err
		}
	}
	if e.saved, err = e.updateFn(left, right); err != nil {
		return nil, err
	}
	return e.saved, nil
}

func (e *BinaryExpression) Merge(arg IExpression) (datavalues.IDataValue, error) {
	var err error
	var left, right datavalues.IDataValue

	other := arg.(*BinaryExpression)

	if left, err = e.left.Merge(other.left); err != nil {
		return nil, err
	}

	if right, err = e.right.Merge(other.right); err != nil {
		return nil, err
	}

	return e.updateFn(left, right)
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
