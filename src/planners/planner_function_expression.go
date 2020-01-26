// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package planners

import (
	"fmt"
)

type FunctionExpressionPlan struct {
	FuncName string
	Args     []IPlan
}

func NewFunctionExpressionPlan(funcName string, args ...IPlan) *FunctionExpressionPlan {
	return &FunctionExpressionPlan{
		FuncName: funcName,
		Args:     args,
	}
}

func (plan *FunctionExpressionPlan) Name() string {
	return "FuncExpressionNode"
}

func (plan *FunctionExpressionPlan) Build() error {
	return nil
}

func (plan *FunctionExpressionPlan) Walk(visit Visit) error {
	return Walk(visit, plan.Args...)
}

func (plan *FunctionExpressionPlan) String() string {
	res := plan.Name()
	res += fmt.Sprintf("=(Func=[%s], Args=[%+v])", plan.FuncName, plan.Args)
	return res
}
