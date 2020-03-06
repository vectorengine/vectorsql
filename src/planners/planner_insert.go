// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package planners

import (
	"encoding/json"

	"parsers/sqlparser"
)

type InsertPlan struct {
	Name   string
	Schema string
	Table  string
	Format string
}

func NewInsertPlan(ast sqlparser.Statement) IPlan {
	format := ""
	node := ast.(*sqlparser.Insert)

	if node.Formats != nil {
		format = node.Formats.FormatName
	}
	return &InsertPlan{
		Name:   "InsertPlan",
		Schema: node.Table.Qualifier.String(),
		Table:  node.Table.Name.String(),
		Format: format,
	}
}

func (plan *InsertPlan) Build() error {
	return nil
}

func (plan *InsertPlan) Walk(visit Visit) error {
	return nil
}

func (plan *InsertPlan) String() string {
	out, err := json.MarshalIndent(plan, "", "    ")
	if err != nil {
		return err.Error()
	}
	return string(out)
}
