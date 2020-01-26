// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package planners

import (
	"base/errors"
	"parsers/sqlparser"
)

type CreateTablePlan struct {
	Ast *sqlparser.DDL
}

func NewCreateTablePlan(ast sqlparser.Statement) IPlan {
	return &CreateTablePlan{
		Ast: ast.(*sqlparser.DDL),
	}
}

func (plan *CreateTablePlan) Name() string {
	return "CreateTableNode"
}

func (plan *CreateTablePlan) Build() error {
	if plan.Ast.Name() != sqlparser.NodeNameTableCreate {
		return errors.Errorf("CreateTable Plan must be 'Create' operation, got:%s", plan.Ast.Name())
	}
	return nil
}

func (plan *CreateTablePlan) Walk(visit Visit) error {
	return nil
}

func (plan *CreateTablePlan) String() string {
	res := plan.Name()

	buf := sqlparser.NewTrackedBuffer(nil)
	plan.Ast.Format(buf)

	res += "("
	res += "AST: " + buf.String() + "\n"
	res += ")"
	return res
}
