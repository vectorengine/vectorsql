// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package planners

import (
	"encoding/json"
	"fmt"
)

type FilterPlan struct {
	Name    string
	SubPlan IPlan
}

func NewFilterPlan(plan IPlan) *FilterPlan {
	return &FilterPlan{
		Name:    "FilterPlan",
		SubPlan: plan,
	}
}

func (plan *FilterPlan) Build() error {
	// Check Filter plan.
	hasAggregate, err := CheckAggregateExpressions(plan.SubPlan)
	if err != nil {
		return err
	}
	if hasAggregate {
		return fmt.Errorf("Unsupported aggregate expression in Where")
	}
	return plan.SubPlan.Build()
}

func (plan *FilterPlan) Walk(visit Visit) error {
	return Walk(visit, plan.SubPlan)
}

func (plan *FilterPlan) String() string {
	out, err := json.MarshalIndent(plan, "", "    ")
	if err != nil {
		return err.Error()
	}
	return string(out)
}
