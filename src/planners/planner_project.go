// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package planners

import (
	"encoding/json"
)

type ProjectPlan struct {
	Name    string
	SubPlan *MapPlan
}

func NewProjectPlan(plan *MapPlan) *ProjectPlan {
	return &ProjectPlan{
		Name:    "ProjectPlan",
		SubPlan: plan,
	}
}

func (plan *ProjectPlan) Build() error {
	return plan.SubPlan.Build()
}

func (plan *ProjectPlan) Walk(visit Visit) error {
	return Walk(visit, plan.SubPlan)
}

func (plan *ProjectPlan) String() string {
	out, err := json.MarshalIndent(plan, "", "    ")
	if err != nil {
		return err.Error()
	}
	return string(out)
}
