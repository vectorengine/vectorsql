// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package planners

import (
	"parsers"

	"encoding/json"

	"parsers/sqlparser"
)

type ShowTablesPlan struct {
	Name    string
	SubPlan IPlan
}

func NewShowTablesPlan(ast sqlparser.Statement) IPlan {
	return &ShowTablesPlan{
		Name: "ShowTablesPlan",
	}
}

func (plan *ShowTablesPlan) Build() error {
	query := "select * from system.tables"
	ast, err := parsers.Parse(query)
	if err != nil {
		return err
	}
	plan.SubPlan = NewSelectPlan(ast)
	return plan.SubPlan.Build()
}

func (plan *ShowTablesPlan) Walk(visit Visit) error {
	return Walk(visit, plan.SubPlan)
}

func (plan *ShowTablesPlan) String() string {
	out, err := json.MarshalIndent(plan, "", "    ")
	if err != nil {
		return err.Error()
	}
	return string(out)
}
