// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package planners

import (
	"encoding/json"
)

type GroupByPlan struct {
	Name        string
	GroupBys    *MapPlan `json:",omitempty"`
	Aggregators *MapPlan `json:",omitempty"`
}

func NewGroupByPlan(aggregators *MapPlan, groupbys *MapPlan) *GroupByPlan {
	return &GroupByPlan{
		Name:        "GroupByPlan",
		GroupBys:    groupbys,
		Aggregators: aggregators,
	}
}

func (plan *GroupByPlan) Build() error {
	return nil
}

func (plan *GroupByPlan) Walk(visit Visit) error {
	return Walk(visit, plan.GroupBys, plan.Aggregators)
}

func (plan *GroupByPlan) String() string {
	out, err := json.MarshalIndent(plan, "", "    ")
	if err != nil {
		return err.Error()
	}
	return string(out)
}
