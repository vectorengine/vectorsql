// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package planners

import (
	"encoding/json"

	"parsers/sqlparser"
)

type UsePlan struct {
	Name string
	Ast  *sqlparser.Use
}

func NewUsePlan(ast sqlparser.Statement) IPlan {
	return &UsePlan{
		Name: "UsePlan",
		Ast:  ast.(*sqlparser.Use),
	}
}

func (plan *UsePlan) Build() error {
	return nil
}

func (plan *UsePlan) Walk(visit Visit) error {
	return nil
}

func (plan *UsePlan) String() string {
	out, err := json.MarshalIndent(plan, "", "    ")
	if err != nil {
		return err.Error()
	}
	return string(out)
}
