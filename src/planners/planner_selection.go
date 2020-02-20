// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package planners

import (
	"encoding/json"
	"fmt"
)

type SelectionMode string

const (
	NormalSelection    SelectionMode = "NormalSelection"
	AggregateSelection SelectionMode = "AggregateSelection"
	GroupBySelection   SelectionMode = "GroupBySelection"
)

type SelectionPlan struct {
	Name          string
	Projects      *MapPlan `json:",omitempty"`
	GroupBys      *MapPlan `json:",omitempty"`
	SelectionMode SelectionMode
}

func NewSelectionPlan(projects *MapPlan, groupbys *MapPlan) *SelectionPlan {
	return &SelectionPlan{
		Name:          "SelectionPlan",
		Projects:      projects,
		GroupBys:      groupbys,
		SelectionMode: NormalSelection,
	}
}

func (plan *SelectionPlan) Build() error {
	// Check GroupBy plan.
	hasAggregate, err := CheckAggregateExpressions(plan.GroupBys)
	if err != nil {
		return err
	}
	if hasAggregate {
		return fmt.Errorf("Unsupported aggregate expression in GroupBy")
	}

	// Check aggregate mode.
	hasAggregate, err = CheckAggregateExpressions(plan.Projects)
	if err != nil {
		return err
	}
	if hasAggregate {
		plan.SelectionMode = AggregateSelection
	}

	// Check groupby mode.
	if plan.GroupBys.Length() > 0 {
		plan.SelectionMode = GroupBySelection
	}
	return nil
}

func (plan *SelectionPlan) Walk(visit Visit) error {
	return Walk(visit, plan.Projects, plan.GroupBys)
}

func (plan *SelectionPlan) String() string {
	out, err := json.MarshalIndent(plan, "", "    ")
	if err != nil {
		return err.Error()
	}
	return string(out)
}
