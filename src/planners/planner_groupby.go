// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package planners

import (
	"encoding/json"
)

type GroupByPlan struct {
	Name     string
	Projects *MapPlan `json:",omitempty"`
	GroupBys *MapPlan `json:",omitempty"`
}

func NewGroupByPlan(projects *MapPlan, groupbys *MapPlan) *GroupByPlan {
	return &GroupByPlan{
		Name:     "GroupByPlan",
		Projects: projects,
		GroupBys: groupbys,
	}
}

func (plan *GroupByPlan) Build() error {
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
