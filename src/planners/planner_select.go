// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package planners

import (
	"parsers/sqlparser"
)

type SelectPlan struct {
	Ast     *sqlparser.Select
	SubPlan *MapPlan
}

func NewSelectPlan(ast sqlparser.Statement) IPlan {
	return &SelectPlan{
		Ast:     ast.(*sqlparser.Select),
		SubPlan: NewMapPlan(),
	}
}

func (plan *SelectPlan) Name() string {
	return "SelectNode"
}

func (plan *SelectPlan) Build() error {
	ast := plan.Ast
	tree := plan.SubPlan

	// Source.
	source, err := parseTableExpression(ast.From[0])
	if err != nil {
		return err
	}
	tree.Add(source)

	// Project.
	project, err := parseProject(ast.SelectExprs)
	if err != nil {
		return err
	}
	projectPlan := NewProjectPlan(project)
	tree.Add(projectPlan)

	// Filter.
	if ast.Where != nil {
		logic, err := parseLogic(ast.Where.Expr)
		if err != nil {
			return err
		}
		filterPlan := NewFilterPlan(logic)
		tree.Add(filterPlan)
	}

	// OrderBy.
	if ast.OrderBy != nil {
		orders, err := parseOrderByExpressions(ast.OrderBy)
		if err != nil {
			return err
		}
		orderByPlan := NewOrderByPlan(orders...)
		tree.Add(orderByPlan)
	}

	// Sink.
	tree.Add(NewSinkPlan())
	return tree.Build()
}

func (plan *SelectPlan) Walk(visit Visit) error {
	return Walk(visit, plan.SubPlan)
}

func (plan *SelectPlan) String() string {
	return plan.SubPlan.String()
}
