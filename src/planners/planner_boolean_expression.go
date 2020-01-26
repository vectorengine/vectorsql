// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package planners

import (
	"fmt"
)

type BooleanExpressionPlan struct {
	Args     []IPlan
	FuncName string
}

func NewBooleanExpressionPlan(fnname string, args ...IPlan) *BooleanExpressionPlan {
	return &BooleanExpressionPlan{
		FuncName: fnname,
		Args:     args,
	}
}

func (plan *BooleanExpressionPlan) Name() string {
	return "BooleanExpressionNode"
}

func (plan *BooleanExpressionPlan) Build() error {
	return nil
}

func (plan *BooleanExpressionPlan) Walk(visit Visit) error {
	return Walk(visit, plan.Args...)
}

func (plan *BooleanExpressionPlan) String() string {
	res := plan.Name()
	res += fmt.Sprintf("=(Func=[%s], Args=[%+v])", plan.FuncName, plan.Args)
	return res
}
