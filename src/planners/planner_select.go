// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package planners

import (
	"encoding/json"

	"parsers/sqlparser"
)

type SelectPlan struct {
	Name    string
	SubPlan *MapPlan `json:",omitempty"`
	ast     *sqlparser.Select
}

func NewSelectPlan(ast sqlparser.Statement) IPlan {
	return &SelectPlan{
		Name:    "SelectPlan",
		ast:     ast.(*sqlparser.Select),
		SubPlan: NewMapPlan(),
	}
}

func (plan *SelectPlan) Build() error {
	ast := plan.ast
	tree := plan.SubPlan

	// Source.
	source, err := parseFrom(ast.From[0])
	if err != nil {
		return err
	}
	tree.Add(source)

	// Fields.
	fields, _ := parseFields(nil, ast.SelectExprs)

	// Aliases.
	aliases, err := parseAliases(fields)
	if err != nil {
		return err
	}

	// Fields with aliased.
	fields, err = parseFields(aliases, ast.SelectExprs)
	if err != nil {
		return err
	}

	// Filter.
	if ast.Where != nil {
		logic, err := parseWhere(aliases, ast.Where.Expr)
		if err != nil {
			return err
		}
		filterPlan := NewFilterPlan(logic)
		tree.Add(filterPlan)
	}

	// Selection.
	{
		groupby, err := parseGroupBy(aliases, ast.GroupBy)
		if err != nil {
			return err
		}
		selectionPlan := NewSelectionPlan(fields, groupby)
		tree.Add(selectionPlan)
	}

	// Having.
	if ast.Having != nil {
		logic, err := parseWhere(nil, ast.Having.Expr)
		if err != nil {
			return err
		}
		havingPlan := NewFilterPlan(logic)
		tree.Add(havingPlan)
	}

	// OrderBy.
	if ast.OrderBy != nil {
		orders, err := parseOrderBy(ast.OrderBy)
		if err != nil {
			return err
		}
		orderByPlan := NewOrderByPlan(orders...)
		tree.Add(orderByPlan)
	}

	// Limit
	if ast.Limit != nil {
		limitPlan, err := parseLimit(ast.Limit)
		if err != nil {
			return err
		}
		tree.Add(limitPlan)
	}

	// Projection.
	if fields.Length() > 0 {
		projectPlan := NewProjectPlan(fields)
		tree.Add(projectPlan)
	}

	// Sink.
	tree.Add(NewSinkPlan())
	return tree.Build()
}

func (plan *SelectPlan) Walk(visit Visit) error {
	return Walk(visit, plan.SubPlan)
}

func (plan *SelectPlan) String() string {
	out, err := json.MarshalIndent(plan, "", "    ")
	if err != nil {
		return err.Error()
	}
	return string(out)
}
