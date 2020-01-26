// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package planners

import (
	"fmt"
)

type TableValuedFunctionExpressionPlan struct {
	FuncName string
	SubPlan  IPlan
}

func NewTableValuedFunctionExpressionPlan(name string, plan IPlan) *TableValuedFunctionExpressionPlan {
	return &TableValuedFunctionExpressionPlan{
		FuncName: name,
		SubPlan:  plan,
	}
}

func (plan *TableValuedFunctionExpressionPlan) Name() string {
	return "TableValuedFunctionExpressionNode"
}

func (plan *TableValuedFunctionExpressionPlan) Build() error {
	return plan.SubPlan.Build()
}

func (plan *TableValuedFunctionExpressionPlan) Walk(visit Visit) error {
	return Walk(visit, plan.SubPlan)
}

func (plan *TableValuedFunctionExpressionPlan) String() string {
	res := plan.Name()
	res += fmt.Sprintf("=(Func=[%s], Args=[%+v])", plan.FuncName, plan.SubPlan)
	return res

}
