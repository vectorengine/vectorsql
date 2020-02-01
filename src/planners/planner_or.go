// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package planners

import (
	"encoding/json"
)

type OrPlan struct {
	Name     string
	FuncName string
	Left     IPlan
	Right    IPlan
}

func NewOrPlan(args ...IPlan) *OrPlan {
	return &OrPlan{
		Name:     "OrPlan",
		FuncName: "OR",
		Left:     args[0],
		Right:    args[1],
	}
}

func (plan *OrPlan) Build() error {
	return nil
}

func (plan *OrPlan) Walk(visit Visit) error {
	return Walk(visit, plan.Left, plan.Right)
}

func (plan *OrPlan) String() string {
	out, err := json.MarshalIndent(plan, "", "    ")
	if err != nil {
		return err.Error()
	}
	return string(out)
}
