// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package planners

import ()

type FilterPlan struct {
	SubPlan IPlan
}

func NewFilterPlan(plan IPlan) *FilterPlan {
	return &FilterPlan{
		SubPlan: plan,
	}
}

func (plan *FilterPlan) Name() string {
	return "FilterNode"
}

func (plan *FilterPlan) Build() error {
	return plan.SubPlan.Build()
}

func (plan *FilterPlan) Walk(visit Visit) error {
	return Walk(visit, plan.SubPlan)
}

func (plan *FilterPlan) String() string {
	res := "\n"
	res += "->"
	res += plan.Name()
	res += "\t--> "
	res += "("
	res += plan.SubPlan.String()
	res += ")"
	return res
}
