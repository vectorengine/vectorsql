// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package planners

import (
	"fmt"

	"base/errors"
	"parsers/sqlparser"
)

type DropDatabasePlan struct {
	Ast *sqlparser.DBDDL
}

func NewDropDatabasePlan(ast sqlparser.Statement) IPlan {
	return &DropDatabasePlan{
		Ast: ast.(*sqlparser.DBDDL),
	}
}

func (plan *DropDatabasePlan) Name() string {
	return "DropDatabaseNode"
}

func (plan *DropDatabasePlan) Build() error {
	if plan.Ast.Name() != sqlparser.NodeNameDatabaseDrop {
		return errors.Errorf("DropDatabase Plan must be 'Drop' operation, got:%s", plan.Ast.Name())
	}
	return nil
}

func (plan *DropDatabasePlan) Walk(visit Visit) error {
	return nil
}

func (plan *DropDatabasePlan) String() string {
	res := plan.Name()
	res += "("
	res += "AST: " + fmt.Sprintf("%+v", plan.Ast) + "\n"
	res += ")"
	return res
}
