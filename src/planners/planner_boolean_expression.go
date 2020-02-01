// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package planners

import (
	"encoding/json"
)

type BooleanExpressionPlan struct {
	Name     string
	Args     []IPlan
	FuncName string
}

func NewBooleanExpressionPlan(fnname string, args ...IPlan) *BooleanExpressionPlan {
	return &BooleanExpressionPlan{
		Name:     "BooleanExpressionPlan",
		FuncName: fnname,
		Args:     args,
	}
}

func (plan *BooleanExpressionPlan) Build() error {
	return nil
}

func (plan *BooleanExpressionPlan) Walk(visit Visit) error {
	return Walk(visit, plan.Args...)
}

func (plan *BooleanExpressionPlan) String() string {
	out, err := json.MarshalIndent(plan, "", "    ")
	if err != nil {
		return err.Error()
	}
	return string(out)
}
