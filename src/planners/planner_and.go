// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package planners

import (
	"fmt"
)

type AndPlan struct {
	FuncName string
	Left     IPlan
	Right    IPlan
}

func NewAndPlan(args ...IPlan) *AndPlan {
	return &AndPlan{
		FuncName: "AND",
		Left:     args[0],
		Right:    args[1],
	}
}

func (plan *AndPlan) Name() string {
	return "AndNode"
}

func (plan *AndPlan) Build() error {
	return nil
}

func (plan *AndPlan) Walk(visit Visit) error {
	return Walk(visit, plan.Left, plan.Right)
}

func (plan *AndPlan) String() string {
	res := plan.Name()
	res += fmt.Sprintf("=(Func=[%s], Left=[%+v], Right=[%+v])", plan.FuncName, plan.Left, plan.Right)
	return res
}
