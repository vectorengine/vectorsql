// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package planners

import (
	"expressions"

	"base/errors"
)

type IPlan interface {
	Build() error
	Walk(visit Visit) error
	String() string
}

type Visit func(plan IPlan) (kontinue bool, err error)

func Walk(visit Visit, plans ...IPlan) error {
	for _, plan := range plans {
		if plan == nil {
			continue
		}
		kontinue, err := visit(plan)
		if err != nil {
			return err
		}
		if kontinue {
			err = plan.Walk(visit)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func BuildExpressions(plan IPlan) (expressions.IExpression, error) {
	var expr expressions.IExpression

	switch t := plan.(type) {
	case *BinaryExpressionPlan:
		left, err := BuildExpressions(t.Left)
		if err != nil {
			return nil, err
		}
		right, err := BuildExpressions(t.Right)
		if err != nil {
			return nil, err
		}
		if expr, err = expressions.BinaryExpressionFactory(t.FuncName, left, right); err != nil {
			return nil, err
		}
		return expr, nil
	case *VariablePlan:
		return expressions.VAR(string(t.Value)), nil
	case *ConstantPlan:
		return expressions.CONST(t.Value), nil
	default:
		return nil, errors.Errorf("Unsupported plan:%s", t)
	}
}
