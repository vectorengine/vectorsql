// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package planners

import (
	"encoding/json"

	"base/errors"
	"parsers/sqlparser"
)

type DropTablePlan struct {
	Name string
	Ast  *sqlparser.DDL
}

func NewDropTablePlan(ast sqlparser.Statement) IPlan {
	return &DropTablePlan{
		Name: "DropTablePlan",
		Ast:  ast.(*sqlparser.DDL),
	}
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
	out, err := json.MarshalIndent(plan, "", "    ")
	if err != nil {
		return err.Error()
	}
	return string(out)
}
