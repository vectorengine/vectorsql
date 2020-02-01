// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package planners

import (
	"encoding/json"
)

type GroupByPlan struct {
	Name    string
	SubPlan *MapPlan
}

func NewGroupByPlan(plan *MapPlan) *GroupByPlan {
	return &GroupByPlan{
		Name:    "GroupByPlan",
		SubPlan: plan,
	}
}

func (plan *GroupByPlan) Build() error {
	return plan.SubPlan.Build()
}

func (plan *GroupByPlan) Walk(visit Visit) error {
	return Walk(visit, plan.SubPlan)
}

func (plan *GroupByPlan) String() string {
	out, err := json.MarshalIndent(plan, "", "    ")
	if err != nil {
		return err.Error()
	}
	return string(out)
}
