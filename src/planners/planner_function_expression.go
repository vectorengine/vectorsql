// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package planners

import (
	"encoding/json"
)

type FunctionExpressionPlan struct {
	Name     string
	FuncName string
	Args     []IPlan
}

func NewFunctionExpressionPlan(funcName string, args ...IPlan) *FunctionExpressionPlan {
	return &FunctionExpressionPlan{
		Name:     "FunctionExpressionPlan",
		FuncName: funcName,
		Args:     args,
	}
}

func (plan *FunctionExpressionPlan) Build() error {
	return nil
}

func (plan *FunctionExpressionPlan) Walk(visit Visit) error {
	return Walk(visit, plan.Args...)
}

func (plan *FunctionExpressionPlan) String() string {
	out, err := json.MarshalIndent(plan, "", "    ")
	if err != nil {
		return err.Error()
	}
	return string(out)
}
