// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package planners

import (
	"encoding/json"
)

type LimitPlan struct {
	Name         string
	OffsetPlan   IPlan
	RowcountPlan IPlan
}

func NewLimitPlan(offset, rowcount IPlan) *LimitPlan {
	return &LimitPlan{
		Name:         "LimitPlan",
		OffsetPlan:   offset,
		RowcountPlan: rowcount,
	}
}

func (plan *LimitPlan) Build() error {
	return nil
}

func (plan *LimitPlan) Walk(visit Visit) error {
	return Walk(visit, plan.OffsetPlan, plan.RowcountPlan)
}

func (plan *LimitPlan) String() string {
	out, err := json.MarshalIndent(plan, "", "    ")
	if err != nil {
		return err.Error()
	}
	return string(out)
}
