// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package planners

import (
	"encoding/json"

	"base/errors"
	"parsers/sqlparser"
)

type CreateTablePlan struct {
	Name string
	Ast  *sqlparser.DDL
}

func NewCreateTablePlan(ast sqlparser.Statement) IPlan {
	return &CreateTablePlan{
		Name: "CreateTablePlan",
		Ast:  ast.(*sqlparser.DDL),
	}
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
	out, err := json.MarshalIndent(plan, "", "    ")
	if err != nil {
		return err.Error()
	}
	return string(out)
}
