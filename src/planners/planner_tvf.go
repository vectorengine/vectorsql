// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package planners

import (
	"encoding/json"
)

type TableValuedFunctionPlan struct {
	Name     string
	As       string
	FuncName string
	SubPlan  *MapPlan
}

func NewTableValuedFunctionPlan(name string, plan *MapPlan) *TableValuedFunctionPlan {
	return &TableValuedFunctionPlan{
		Name:     "TableValuedFunctionPlan",
		FuncName: name,
		SubPlan:  plan,
	}
}

func (plan *TableValuedFunctionPlan) Build() error {
	return plan.SubPlan.Build()
}

func (plan *TableValuedFunctionPlan) Walk(visit Visit) error {
	return Walk(visit, plan.SubPlan)
}

func (plan *TableValuedFunctionPlan) String() string {
	out, err := json.MarshalIndent(plan, "", "    ")
	if err != nil {
		return err.Error()
	}
	return string(out)
}
