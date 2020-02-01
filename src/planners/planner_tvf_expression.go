// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package planners

import (
	"encoding/json"
)

type TableValuedFunctionExpressionPlan struct {
	Name     string
	FuncName string
	SubPlan  IPlan
}

func NewTableValuedFunctionExpressionPlan(name string, plan IPlan) *TableValuedFunctionExpressionPlan {
	return &TableValuedFunctionExpressionPlan{
		Name:     "TableValuedFunctionExpressionPlan",
		FuncName: name,
		SubPlan:  plan,
	}
}

func (plan *TableValuedFunctionExpressionPlan) Build() error {
	return plan.SubPlan.Build()
}

func (plan *TableValuedFunctionExpressionPlan) Walk(visit Visit) error {
	return Walk(visit, plan.SubPlan)
}

func (plan *TableValuedFunctionExpressionPlan) String() string {
	out, err := json.MarshalIndent(plan, "", "    ")
	if err != nil {
		return err.Error()
	}
	return string(out)
}
