// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package planners

import (
	"encoding/json"

	"parsers/sqlparser"
)

type ShowTablesPlan struct {
	Name    string
	SubPlan IPlan
	Ast     *sqlparser.Show
}

func NewShowTablesPlan(ast sqlparser.Statement) IPlan {
	show := ast.(*sqlparser.Show)
	return &ShowTablesPlan{
		Name: "ShowTablesPlan",
		Ast:  show,
	}
}

func (plan *ShowTablesPlan) Build() error {
	opt := plan.Ast.ShowTablesOpt

	// check the plan
	if opt != nil && opt.Filter != nil && opt.Filter.Filter != nil {
		_, err := parseExpression(nil, opt.Filter.Filter)
		if err != nil {
			return err
		}
	}

	if plan.Ast.Limit != nil {
		_, err := parseLimit(plan.Ast.Limit)
		if err != nil {
			return err
		}
	}
	return nil
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
