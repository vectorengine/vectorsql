// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package planners

import ()

type ProjectPlan struct {
	SubPlan *MapPlan
}

func NewProjectPlan(plan *MapPlan) *ProjectPlan {
	return &ProjectPlan{
		SubPlan: plan,
	}
}

func (plan *ProjectPlan) Name() string {
	return "ProjectNode"
}

func (plan *ProjectPlan) Build() error {
	return plan.SubPlan.Build()
}

func (plan *ProjectPlan) Walk(visit Visit) error {
	return Walk(visit, plan.SubPlan)
}

func (plan *ProjectPlan) String() string {
	res := "\n"
	res += "->"
	res += plan.Name()
	res += "\t--> "
	res += "("
	res += plan.SubPlan.String()
	res += ")"
	return res
}
