// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package planners

import (
	"encoding/json"
)

type MapPlan struct {
	Name     string
	SubPlans []IPlan `json:",omitempty"`
}

func NewMapPlan(plans ...IPlan) *MapPlan {
	return &MapPlan{
		Name:     "MapPlan",
		SubPlans: plans,
	}
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
	out, err := json.MarshalIndent(plan, "", "    ")
	if err != nil {
		return err.Error()
	}
	return string(out)

}

func (plan *MapPlan) Add(p IPlan) {
	plan.SubPlans = append(plan.SubPlans, p)
}

func (plan *MapPlan) AsPlans() []IPlan {
	return plan.SubPlans
}
