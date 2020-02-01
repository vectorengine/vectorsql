// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package planners

import (
	"encoding/json"
)

type AndPlan struct {
	Name     string
	FuncName string
	Left     IPlan
	Right    IPlan
}

func NewAndPlan(args ...IPlan) *AndPlan {
	return &AndPlan{
		Name:     "AndPlan",
		FuncName: "AND",
		Left:     args[0],
		Right:    args[1],
	}
}

func (plan *AndPlan) Build() error {
	return nil
}

func (plan *AndPlan) Walk(visit Visit) error {
	return Walk(visit, plan.Left, plan.Right)
}

func (plan *AndPlan) String() string {
	out, err := json.MarshalIndent(plan, "", "    ")
	if err != nil {
		return err.Error()
	}
	return string(out)
}
