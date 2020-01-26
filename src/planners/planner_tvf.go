// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package planners

import (
	"fmt"
)

type TableValuedFunctionPlan struct {
	As       string
	FuncName string
	SubPlan  *MapPlan
}

func NewTableValuedFunctionPlan(name string, plan *MapPlan) *TableValuedFunctionPlan {
	return &TableValuedFunctionPlan{
		FuncName: name,
		SubPlan:  plan,
	}
}

func (plan *TableValuedFunctionPlan) Name() string {
	return "TableValuedFunctionNode"
}

func (plan *TableValuedFunctionPlan) Build() error {
	return plan.SubPlan.Build()
}

func (plan *TableValuedFunctionPlan) Walk(visit Visit) error {
	return Walk(visit, plan.SubPlan)
}

func (plan *TableValuedFunctionPlan) String() string {
	res := "\n"
	res += "->"
	res += plan.Name()
	res += "\t--> "
	res += fmt.Sprintf("(Func=[%s], Args=[%+v])", plan.FuncName, plan.SubPlan)
	return res
}
