// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package planners

import (
	"base/errors"
	"parsers/sqlparser"
)

type DropTablePlan struct {
	Ast *sqlparser.DDL
}

func NewDropTablePlan(ast sqlparser.Statement) IPlan {
	return &DropTablePlan{
		Ast: ast.(*sqlparser.DDL),
	}
}

func (plan *DropTablePlan) Name() string {
	return "DropTableNode"
}

func (plan *DropTablePlan) Build() error {
	if plan.Ast.Name() != sqlparser.NodeNameTableDrop {
		return errors.Errorf("DropTable Plan must be 'Drop' operation, got:%s", plan.Ast.Name())
	}
	return nil
}

func (plan *DropTablePlan) Walk(visit Visit) error {
	return nil
}

func (plan *DropTablePlan) String() string {
	res := plan.Name()

	buf := sqlparser.NewTrackedBuffer(nil)
	plan.Ast.Format(buf)

	res += "("
	res += "AST: " + buf.String() + "\n"
	res += ")"
	return res
}
