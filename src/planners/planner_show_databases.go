// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package planners

import (
	"parsers"

	"encoding/json"

	"parsers/sqlparser"
)

type ShowDatabasesPlan struct {
	Name    string
	SubPlan IPlan
}

func NewShowDatabasesPlan(ast sqlparser.Statement) IPlan {
	return &ShowDatabasesPlan{
		Name: "ShowDatabasesPlan",
	}
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
	out, err := json.MarshalIndent(plan, "", "    ")
	if err != nil {
		return err.Error()
	}
	return string(out)
}
