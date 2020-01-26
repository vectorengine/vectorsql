// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package planners

import (
	"fmt"
)

type OrPlan struct {
	FuncName string
	Left     IPlan
	Right    IPlan
}

func NewOrPlan(args ...IPlan) *OrPlan {
	return &OrPlan{
		FuncName: "OR",
		Left:     args[0],
		Right:    args[1],
	}
}

func (plan *OrPlan) Name() string {
	return "OrNode"
}

func (plan *OrPlan) Build() error {
	return nil
}

func (plan *OrPlan) Walk(visit Visit) error {
	return Walk(visit, plan.Left, plan.Right)
}

func (plan *OrPlan) String() string {
	res := plan.Name()
	res += fmt.Sprintf("=(Func=[%s], Left=[%+v], Right=[%+v])", plan.FuncName, plan.Left, plan.Right)
	return res
}
