// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package planners

import (
	"fmt"
)

type ConstantPlan struct {
	Value interface{}
}

func NewConstantPlan(value interface{}) *ConstantPlan {
	return &ConstantPlan{
		Value: value,
	}
}

func (plan *ConstantPlan) Name() string {
	return "ConstantNode"
}

func (plan *ConstantPlan) Build() error {
	return nil
}

func (plan *ConstantPlan) Walk(visit Visit) error {
	return nil
}

func (plan *ConstantPlan) String() string {
	res := plan.Name()
	res += fmt.Sprintf("=<%+v>", plan.Value)
	return res
}
