// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package planners

import (
	"fmt"
)

type VariablePlan struct {
	Value string
}

func NewVariablePlan(value string) *VariablePlan {
	return &VariablePlan{
		Value: value,
	}
}

func (plan *VariablePlan) Name() string {
	return "VariableNode"
}

func (plan *VariablePlan) Build() error {
	return nil
}

func (plan *VariablePlan) Walk(visit Visit) error {
	return nil
}

func (plan *VariablePlan) String() string {
	res := plan.Name()
	res += fmt.Sprintf("=[$%+v]", plan.Value)
	return res
}
