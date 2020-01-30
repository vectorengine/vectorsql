// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package planners

import (
	"parsers"

	"parsers/sqlparser"
)

type ShowDatabasesPlan struct {
	Ast     *sqlparser.Show
	SubPlan IPlan
}

func NewShowDatabasesPlan(ast sqlparser.Statement) IPlan {
	return &ShowDatabasesPlan{Ast: ast.(*sqlparser.Show)}
}

func (plan *ShowDatabasesPlan) Name() string {
	return "ShowDatabasesNode"
}

func (plan *ShowDatabasesPlan) Build() error {
	query := "select * from system.databases order by name asc"
	ast, err := parsers.Parse(query)
	if err != nil {
		return err
	}
	plan.SubPlan = NewSelectPlan(ast)
	return plan.SubPlan.Build()
}

func (plan *ShowDatabasesPlan) Walk(visit Visit) error {
	return Walk(visit, plan.SubPlan)
}

func (plan *ShowDatabasesPlan) String() string {
	res := plan.Name()

	buf := sqlparser.NewTrackedBuffer(nil)
	plan.Ast.Format(buf)

	res += "("
	res += "AST: " + buf.String() + "\n"
	res += ")"
	return res
}
