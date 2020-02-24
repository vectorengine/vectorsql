// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package planners

import (
	"base/errors"
	"expressions"
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

func BuildVariableValues(plan IPlan) ([]string, error) {
	var vars []string

	if err := Walk(func(p IPlan) (bool, error) {
		switch t := p.(type) {
		case *VariablePlan:
			vars = append(vars, t.Value)
			return true, nil
		}
		return true, nil
	}, plan); err != nil {
		return nil, err
	}
	return vars, nil
}

func BuildExpressions(plan *MapPlan) ([]expressions.IExpression, error) {
	exps := make([]expressions.IExpression, plan.Length())
	for i, p := range plan.SubPlans {
		expr, err := BuildExpression(p)
		if err != nil {
			return nil, err
		}
		exps[i] = expr
	}
	return exps, nil
}

func BuildExpression(plan IPlan) (expressions.IExpression, error) {
	switch t := plan.(type) {
	case *VariablePlan:
		return expressions.VAR(string(t.Value)), nil
	case *ConstantPlan:
		return expressions.CONST(t.Value), nil
	case *AliasedExpressionPlan:
		expr, err := BuildExpression(t.Expr)
		if err != nil {
			return nil, err
		}
		return expressions.ALIASED(t.As, expr), nil
	case *UnaryExpressionPlan:
		expr, err := BuildExpression(t.Expr)
		if err != nil {
			return nil, err
		}
		return expressions.ExpressionFactory(t.FuncName, []interface{}{expr})
	case *BinaryExpressionPlan:
		left, err := BuildExpression(t.Left)
		if err != nil {
			return nil, err
		}
		right, err := BuildExpression(t.Right)
		if err != nil {
			return nil, err
		}
		return expressions.ExpressionFactory(t.FuncName, []interface{}{left, right})
	case *FunctionExpressionPlan:
		exprArgs := make([]interface{}, len(t.Args))
		for i, arg := range t.Args {
			expr, err := BuildExpression(arg)
			if err != nil {
				return nil, err
			}
			exprArgs[i] = expr
		}
		return expressions.ExpressionFactory(t.FuncName, exprArgs)
	default:
		return nil, errors.Errorf("Unsupported expression plan:%T", t)
	}
}

func CheckAggregateExpressions(plan IPlan) (bool, error) {
	hasAggregate := false
	if err := Walk(func(p IPlan) (bool, error) {
		switch t := p.(type) {
		case *UnaryExpressionPlan:
			expr, err := expressions.ExpressionFactory(t.FuncName, []interface{}{"NULL"})
			if err != nil {
				return false, err
			}
			switch expr.(type) {
			case *expressions.AggregateExpression:
				hasAggregate = true
				return false, nil
			}
		}
		return true, nil
	}, plan); err != nil {
		return false, err
	}
	return hasAggregate, nil
}
