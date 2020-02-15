// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package planners

import (
	"encoding/json"
	"fmt"
)

type GroupByPlan struct {
	Name         string
	HasAggregate bool
	Projects     *MapPlan `json:",omitempty"`
	GroupBys     *MapPlan `json:",omitempty"`
}

func NewGroupByPlan(projects *MapPlan, groupbys *MapPlan) *GroupByPlan {
	return &GroupByPlan{
		Name:     "GroupByPlan",
		Projects: projects,
		GroupBys: groupbys,
	}
}

func (plan *GroupByPlan) Build() error {
	// Check GroupBy plan.
	hasAggregate, err := CheckAggregateExpressions(plan.GroupBys)
	if err != nil {
		return err
	}
	if hasAggregate {
		return fmt.Errorf("Unsupported aggregate expression in GroupBy")
	}

	// Check Project plan.
	hasAggregate, err = CheckAggregateExpressions(plan.Projects)
	if err != nil {
		return err
	}
	plan.HasAggregate = hasAggregate
	return nil
}

func (plan *GroupByPlan) Walk(visit Visit) error {
	return Walk(visit, plan.Projects, plan.GroupBys)
}

func (plan *GroupByPlan) String() string {
	out, err := json.MarshalIndent(plan, "", "    ")
	if err != nil {
		return err.Error()
	}
	return string(out)
}
