// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package planners

import (
	"encoding/json"
	"parsers"

	"parsers/sqlparser"
)

type ShowTablesPlan struct {
	Name    string
	SubPlan IPlan
	ast     *sqlparser.Show
}

func NewShowTablesPlan(ast sqlparser.Statement) IPlan {
	show := ast.(*sqlparser.Show)
	return &ShowTablesPlan{
		Name: "ShowTablesPlan",
		ast:  show,
	}
}

func (plan *ShowTablesPlan) Build() error {
	query := "select * from system.tables"
	ast, err := parsers.Parse(query)
	if err != nil {
		return err
	}

	opt := plan.ast.ShowTablesOpt
	// check the plan
	if opt != nil && opt.Filter != nil && opt.Filter.Filter != nil {
		_, err := parseExpression(nil, opt.Filter.Filter)
		if err != nil {
			return err
		}
	}

	if plan.ast.Limit != nil {
		_, err := parseLimit(plan.ast.Limit)
		if err != nil {
			return err
		}
	}

	plan.SubPlan = NewSelectPlan(ast)
	return plan.SubPlan.Build()
}

func (plan *ShowTablesPlan) GetAst() *sqlparser.Show {
	return plan.ast
}

func (plan *ShowTablesPlan) Walk(visit Visit) error {
	return Walk(visit, plan.SubPlan)
}

func (plan *ShowTablesPlan) String() string {
	out, err := json.MarshalIndent(plan, "", "    ")
	if err != nil {
		return err.Error()
	}
	return string(out)
}
