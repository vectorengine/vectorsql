// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package planners

import (
	"fmt"

	"base/errors"
	"parsers/sqlparser"
)

type CreateDatabasePlan struct {
	Ast *sqlparser.DBDDL
}

func NewCreateDatabasePlan(ast sqlparser.Statement) IPlan {
	return &CreateDatabasePlan{
		Ast: ast.(*sqlparser.DBDDL),
	}
}

func (plan *CreateDatabasePlan) Name() string {
	return "CreateDatabaseNode"
}

func (plan *CreateDatabasePlan) Build() error {
	if plan.Ast.Name() != sqlparser.NodeNameDatabaseCreate {
		return errors.Errorf("CreateDatabase Plan must be 'Create' operation, got:%s", plan.Ast.Name())
	}
	return nil
}

func (plan *CreateDatabasePlan) Walk(visit Visit) error {
	return nil
}

func (plan *CreateDatabasePlan) String() string {
	res := plan.Name()
	res += "("
	res += "AST: " + fmt.Sprintf("%+v", plan.Ast) + "\n"
	res += ")"
	return res
}
