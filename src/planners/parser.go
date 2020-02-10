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
		switch len(expr.Exprs) {
		case 1:
			expr, err := parseFunctionArgument(expr.Exprs[0].(*sqlparser.AliasedExpr))
			if err != nil {
				return nil, err
			}
			return NewUnaryExpressionPlan(funcName, expr), nil
		case 2:
			left, err := parseFunctionArgument(expr.Exprs[0].(*sqlparser.AliasedExpr))
			if err != nil {
				return nil, err
			}
			right, err := parseFunctionArgument(expr.Exprs[1].(*sqlparser.AliasedExpr))
			if err != nil {
				return nil, err
			}
			return NewBinaryExpressionPlan(funcName, left, right), nil
		default:
			args := make([]IPlan, len(expr.Exprs))
			for i, expr := range expr.Exprs {
				aliased, ok := expr.(*sqlparser.AliasedExpr)
				if !ok {
					return nil, errors.Errorf("Unsupported argument %v of type %v", expr, reflect.TypeOf(expr))
				}
				arg, err := parseFunctionArgument(aliased)
				if err != nil {
					return nil, err
				}
				args[i] = arg
			}
			return NewFunctionExpressionPlan(funcName, args...), nil
		}
	case *sqlparser.BinaryExpr:
		left, err := parseExpression(expr.Left)
		if err != nil {
			return nil, err
		}
		right, err := parseExpression(expr.Right)
		if err != nil {
			return nil, err
		}
		return NewBinaryExpressionPlan(expr.Operator, left, right), nil
	case *sqlparser.ComparisonExpr:
		left, err := parseExpression(expr.Left)
		if err != nil {
			return nil, err
		}
		right, err := parseExpression(expr.Right)
		if err != nil {
			return nil, err
		}
		return NewBinaryExpressionPlan(expr.Operator, left, right), nil
	case *sqlparser.OrExpr:
		left, err := parseExpression(expr.Left)
		if err != nil {
			return nil, err
		}
		right, err := parseExpression(expr.Right)
		if err != nil {
			return nil, err
		}
		return NewBinaryExpressionPlan("OR", left, right), nil
	case *sqlparser.AndExpr:
		left, err := parseExpression(expr.Left)
		if err != nil {
			return nil, err
		}
		right, err := parseExpression(expr.Right)
		if err != nil {
			return nil, err
		}
		return NewBinaryExpressionPlan("AND", left, right), nil
	case *sqlparser.ParenExpr:
		return parseExpression(expr.Expr)
	}
	return nil, errors.Errorf("Unsupported expression %+v %+v", expr, reflect.TypeOf(expr))
}

func parseFunctionArgument(expr *sqlparser.AliasedExpr) (IPlan, error) {
	subExpr, err := parseExpression(expr.Expr)
	if err != nil {
		return nil, errors.Wrapf(err, "Couldn't parse argument")
	}
	return subExpr, nil
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

func parseFrom(expr sqlparser.TableExpr) (IPlan, error) {
	switch expr := expr.(type) {
	case *sqlparser.AliasedTableExpr:
		return parseAliasedTableExpression(expr)
	case *sqlparser.ParenTableExpr:
		return parseFrom(expr.Exprs[0])
	case *sqlparser.TableValuedFunction:
		return parseTableValuedFunction(expr)
	default:
		return nil, errors.Errorf("Unsupported table expression:%+v", expr)
	}
}

func parseFields(sel sqlparser.SelectExprs) (*MapPlan, error) {
	fields := NewMapPlan()

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
			if aliasedExpression.As.String() != "" {
				child = NewAliasedExpressionPlan(aliasedExpression.As.String(), child)
			}
			fields.Add(child)
		}
	}
	return fields, nil
}

func parseWhere(expr sqlparser.Expr) (IPlan, error) {
	return parseExpression(expr)
}

func parseOrderBy(orderBy sqlparser.OrderBy) ([]Order, error) {
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

func parseLimit(limit *sqlparser.Limit) (IPlan, error) {
	if limit.Offset == nil {
		limit.Offset = sqlparser.NewIntVal([]byte("0"))
	}
	offsetPlan, err := parseExpression(limit.Offset)
	if err != nil {
		return nil, errors.Wrapf(err, "Couldn't parse offset")
	}
	rowcountPlan, err := parseExpression(limit.Rowcount)
	if err != nil {
		return nil, errors.Wrapf(err, "Couldn't parse limit")
	}
	return NewLimitPlan(offsetPlan, rowcountPlan), nil
}
