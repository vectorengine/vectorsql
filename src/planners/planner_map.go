// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package planners

import (
	"strings"
)

type MapPlan struct {
	SubPlans []IPlan
}

func NewMapPlan(plans ...IPlan) *MapPlan {
	return &MapPlan{
		SubPlans: plans,
	}
}

func (plan *MapPlan) Name() string {
	return "MapNode"
}

func (plan *MapPlan) Build() error {
	for _, p := range plan.SubPlans {
		if err := p.Build(); err != nil {
			return err
		}
	}
	return nil
}

func (plan *MapPlan) Walk(visit Visit) error {
	return Walk(visit, plan.SubPlans...)
}

func (plan *MapPlan) String() string {
	res := ""
	for _, child := range plan.SubPlans {
		res += child.String()
		res += ", "
	}
	res = strings.TrimRight(res, ", ")
	return res
}

func (plan *MapPlan) Add(p IPlan) {
	plan.SubPlans = append(plan.SubPlans, p)
}
