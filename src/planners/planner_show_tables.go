// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package planners

import (
	"parsers"

	"parsers/sqlparser"
)

type ShowTablesPlan struct {
	Ast     *sqlparser.Show
	SubPlan IPlan
}

func NewShowTablesPlan(ast sqlparser.Statement) IPlan {
	return &ShowTablesPlan{Ast: ast.(*sqlparser.Show)}
}

func (plan *ShowTablesPlan) Name() string {
	return "ShowTablesNode"
}

func (plan *ShowTablesPlan) Build() error {
	query := "select * from system.tables order by name asc"
	ast, err := parsers.Parse(query)
	if err != nil {
		return err
	}
	plan.SubPlan = NewSelectPlan(ast)
	return plan.SubPlan.Build()
}

func (plan *ShowTablesPlan) Walk(visit Visit) error {
	return Walk(visit, plan.SubPlan)
}

func (plan *ShowTablesPlan) String() string {
	res := plan.Name()

	buf := sqlparser.NewTrackedBuffer(nil)
	plan.Ast.Format(buf)

	res += "("
	res += "AST: " + buf.String() + "\n"
	res += ")"
	return res
}
