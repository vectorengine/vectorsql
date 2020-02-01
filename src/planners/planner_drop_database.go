// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package planners

import (
	"encoding/json"

	"base/errors"
	"parsers/sqlparser"
)

type DropDatabasePlan struct {
	Name string
	Ast  *sqlparser.DBDDL
}

func NewDropDatabasePlan(ast sqlparser.Statement) IPlan {
	return &DropDatabasePlan{
		Name: "DropDatabasePlan",
		Ast:  ast.(*sqlparser.DBDDL),
	}
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
	out, err := json.MarshalIndent(plan, "", "    ")
	if err != nil {
		return err.Error()
	}
	return string(out)
}
