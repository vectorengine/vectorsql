// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package planners

import (
	"encoding/json"
)

type AliasedExpressionPlan struct {
	Name string
	As   string
	Expr IPlan
}

func NewAliasedExpressionPlan(as string, expr IPlan) *AliasedExpressionPlan {
	return &AliasedExpressionPlan{
		Name: "AliasedExpressionPlan",
		As:   as,
		Expr: expr,
	}
}

func (plan *AliasedExpressionPlan) Build() error {
	return nil
}

func (plan *AliasedExpressionPlan) Walk(visit Visit) error {
	return Walk(visit, plan.Expr)
}

func (plan *AliasedExpressionPlan) String() string {
	out, err := json.MarshalIndent(plan, "", "    ")
	if err != nil {
		return err.Error()
	}
	return string(out)
}
