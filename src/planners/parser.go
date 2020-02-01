// Copyright 2019 The OctoSQL Authors.
// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package planners

import (
	"reflect"
	"strconv"
	"strings"

	"base/errors"
	"parsers/sqlparser"
)

func parseExpression(expr sqlparser.Expr) (IPlan, error) {
	switch expr := expr.(type) {
	case *sqlparser.ColName:
		name := expr.Name.String()
		return NewVariablePlan(name), nil
	case *sqlparser.SQLVal:
		var err error
		var val interface{}

		switch expr.Type {
		case sqlparser.IntVal:
			var i int64
			i, err = strconv.ParseInt(string(expr.Val), 10, 64)
			val = int(i)
		case sqlparser.FloatVal:
			val, err = strconv.ParseFloat(string(expr.Val), 64)
		case sqlparser.StrVal:
			val = string(expr.Val)
		default:
			err = errors.Errorf("Constant value type unsupported")
		}
		if err != nil {
			return nil, err
		}
		return NewConstantPlan(val), nil
	case *sqlparser.FuncExpr:
		funcName := strings.ToUpper(expr.Name.String())
		arguments := make([]IPlan, 0, 4)
		for _, arg := range expr.Exprs {
			switch arg := arg.(type) {
			case *sqlparser.AliasedExpr:
				logicArg, err := parseFunctionArgument(arg)
				if err != nil {
					return nil, err
				}
				arguments = append(arguments, logicArg)
			default:
				return nil, errors.Errorf("Unsupported argument %v of type %v", arg, reflect.TypeOf(arg))
			}
		}
		return NewFunctionExpressionPlan(funcName, arguments...), nil
	case *sqlparser.BinaryExpr:
		left, err := parseExpression(expr.Left)
		if err != nil {
			return nil, err
		}
		right, err := parseExpression(expr.Right)
		if err != nil {
			return nil, err
		}
		return NewFunctionExpressionPlan(expr.Operator, left, right), nil
	case *sqlparser.ParenExpr:
		return parseExpression(expr.Expr)
	}
	return nil, errors.Errorf("Unsupported expression %+v %+v", expr, reflect.TypeOf(expr))
}

func parseTableExpression(expr sqlparser.TableExpr) (IPlan, error) {
	switch expr := expr.(type) {
	case *sqlparser.AliasedTableExpr:
		return parseAliasedTableExpression(expr)
	case *sqlparser.ParenTableExpr:
		return parseTableExpression(expr.Exprs[0])
	case *sqlparser.TableValuedFunction:
		return parseTableValuedFunction(expr)
	default:
		return nil, errors.Errorf("Unsupported table expression:%+v", expr)
	}
}

func parseAliasedTableExpression(expr *sqlparser.AliasedTableExpr) (IPlan, error) {
	switch subExpr := expr.Expr.(type) {
	case sqlparser.TableName:
		return NewScanPlan(subExpr.Name.String(), subExpr.Qualifier.String()), nil
	default:
		return nil, errors.Errorf("Unsupported aliased table expression:%+v", expr.Expr)
	}
}

func parseTableValuedFunction(expr *sqlparser.TableValuedFunction) (IPlan, error) {
	var arguments []IPlan
	name := expr.Name.String()

	for i := range expr.Args {
		argument, err := parseTableValuedFunctionArgument(expr.Args[i])
		if err != nil {
			return nil, errors.Wrapf(err, "Couldn't parse table valued function argument \"%v\" with index %v", expr.Args[i].Name.String(), i)
		}
		arguments = append(arguments, argument)
	}
	return NewTableValuedFunctionPlan(name, NewMapPlan(arguments...)), nil
}

func parseTableValuedFunctionArgument(expr *sqlparser.TableValuedFunctionArgument) (IPlan, error) {
	name := expr.Name.String()

	switch expr := expr.Value.(type) {
	case *sqlparser.ExprTableValuedFunctionArgumentValue:
		parsed, err := parseExpression(expr.Expr)
		if err != nil {
			return nil, errors.Wrapf(err, "Couldn't parse table valued function argument expression \"%v\"", expr.Expr)
		}
		return NewTableValuedFunctionExpressionPlan(name, parsed), nil
	default:
		return nil, errors.Errorf("Invalid table valued function argument: %v", expr)
	}
}

func parseProject(sel sqlparser.SelectExprs) (*MapPlan, error) {
	tree := NewMapPlan()

	if _, ok := sel[0].(*sqlparser.StarExpr); !ok {
		for i, expr := range sel {
			aliasedExpression, ok := expr.(*sqlparser.AliasedExpr)
			if !ok {
				return nil, errors.Errorf("Expected aliased expression in select on index:%v, got:%+v %+v", i, expr, reflect.TypeOf(expr))
			}
			child, err := parseExpression(aliasedExpression.Expr)
			if err != nil {
				return nil, err
			}
			tree.Add(child)
		}
	}
	return tree, nil
}

func parseGroupBy(groupby sqlparser.GroupBy) (*MapPlan, error) {
	tree := NewMapPlan()

	for i := range groupby {
		sub, err := parseExpression(groupby[i])
		if err != nil {
			return nil, err
		}
		tree.Add(sub)
	}
	return tree, nil
}

func parseLogic(expr sqlparser.Expr) (IPlan, error) {
	switch expr := expr.(type) {
	case *sqlparser.ComparisonExpr:
		return parseComparison(expr.Operator, expr.Left, expr.Right)
	case *sqlparser.AndExpr:
		return parseOperator("AND", expr.Left, expr.Right)
	case *sqlparser.OrExpr:
		return parseOperator("OR", expr.Left, expr.Right)
	case *sqlparser.ParenExpr:
		return parseLogic(expr.Expr)
	default:
		return nil, errors.Errorf("Unsupported logic expression %+v %+v", expr, reflect.TypeOf(expr))
	}
}

func parseComparison(op string, left, right sqlparser.Expr) (IPlan, error) {
	left1, err := parseExpression(left)
	if err != nil {
		return nil, err
	}

	right1, err := parseExpression(right)
	if err != nil {
		return nil, err
	}
	return NewBooleanExpressionPlan(op, left1, right1), nil
}

func parseOperator(op string, left, right sqlparser.Expr) (IPlan, error) {
	left1, err := parseLogic(left)
	if err != nil {
		return nil, err
	}

	right1, err := parseLogic(right)
	if err != nil {
		return nil, err
	}
	switch op {
	case "AND":
		return NewAndPlan(left1, right1), nil
	case "OR":
		return NewOrPlan(left1, right1), nil
	default:
		return NewBooleanExpressionPlan(op, left1, right1), nil
	}
}

func parseFunctionArgument(expr *sqlparser.AliasedExpr) (IPlan, error) {
	subExpr, err := parseExpression(expr.Expr)
	if err != nil {
		return nil, errors.Wrapf(err, "Couldn't parse argument")
	}
	return subExpr, nil
}

func parseOrderByExpressions(orderBy sqlparser.OrderBy) ([]Order, error) {
	orders := make([]Order, len(orderBy))

	for i, field := range orderBy {
		expr, err := parseExpression(field.Expr)
		if err != nil {
			return nil, errors.Errorf("couldn't parse order by expression with index %v", i)
		}
		orders[i].Expression = expr
		orders[i].Direction = field.Direction
	}
	return orders, nil
}
