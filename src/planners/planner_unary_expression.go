// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package planners

import (
	"encoding/json"
)

type UnaryExpressionPlan struct {
	Name     string
	FuncName string
	Expr     IPlan
}

func NewUnaryExpressionPlan(funcName string, expr IPlan) *UnaryExpressionPlan {
	return &UnaryExpressionPlan{
		Name:     "UnaryExpressionPlan",
		FuncName: funcName,
		Expr:     expr,
	}
}

func (plan *UnaryExpressionPlan) Build() error {
	return nil
}

func (plan *UnaryExpressionPlan) Walk(visit Visit) error {
	return Walk(visit, plan.Expr)
}

func (plan *UnaryExpressionPlan) String() string {
	out, err := json.MarshalIndent(plan, "", "    ")
	if err != nil {
		return err.Error()
	}
	return string(out)
}
