// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package planners

import (
	"encoding/json"

	"expressions"
)

type GroupByPlan struct {
	Name         string
	HasAggregate bool
	Projects     *MapPlan `json:",omitempty"`
	GroupBys     *MapPlan `json:",omitempty"`
}

func NewGroupByPlan(projects *MapPlan, groupbys *MapPlan) *GroupByPlan {
	hasAggregate := false
	for _, plan := range projects.SubPlans {
		exprs, err := BuildExpressions(plan)
		if err != nil {
			return nil
		}
		if err := exprs.Walk(func(expr expressions.IExpression) (bool, error) {
			switch expr.(type) {
			case *expressions.AggregateExpression:
				hasAggregate = true
				return false, nil
			}
			return true, nil
		}); err != nil {
			return nil
		}
		if hasAggregate {
			break
		}
	}
	return &GroupByPlan{
		Name:         "GroupByPlan",
		HasAggregate: hasAggregate,
		Projects:     projects,
		GroupBys:     groupbys,
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
