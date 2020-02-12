// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package planners

import (
	"encoding/json"
)

type ProjectionPlan struct {
	Name        string
	Projections *MapPlan `json:",omitempty"`
}

func NewProjectPlan(plan *MapPlan) *ProjectionPlan {
	return &ProjectionPlan{
		Name:        "ProjectionPlan",
		Projections: plan,
	}
}

func (plan *ProjectionPlan) Build() error {
	return plan.Projections.Build()
}

func (plan *ProjectionPlan) Walk(visit Visit) error {
	return Walk(visit, plan.Projections)
}

func (plan *ProjectionPlan) String() string {
	out, err := json.MarshalIndent(plan, "", "    ")
	if err != nil {
		return err.Error()
	}
	return string(out)
}
