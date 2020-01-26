// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package planners

import (
	"parsers/sqlparser"
)

type UsePlan struct {
	Ast *sqlparser.Use
}

func NewUsePlan(ast sqlparser.Statement) IPlan {
	return &UsePlan{
		Ast: ast.(*sqlparser.Use),
	}
}

func (plan *UsePlan) Name() string {
	return "UseNode"
}

func (plan *UsePlan) Build() error {
	return nil
}

func (plan *UsePlan) Walk(visit Visit) error {
	return nil
}

func (plan *UsePlan) String() string {
	res := plan.Name()

	buf := sqlparser.NewTrackedBuffer(nil)
	plan.Ast.Format(buf)

	res += "("
	res += "AST: " + buf.String() + "\n"
	res += ")"
	return res
}
