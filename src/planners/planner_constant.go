// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package planners

import (
	"encoding/json"
)

type ConstantPlan struct {
	Name  string
	Value interface{}
}

func NewConstantPlan(value interface{}) *ConstantPlan {
	return &ConstantPlan{
		Name:  "ConstantPlan",
		Value: value,
	}
}

func (plan *ConstantPlan) Build() error {
	return nil
}

func (plan *ConstantPlan) Walk(visit Visit) error {
	return nil
}

func (plan *ConstantPlan) String() string {
	out, err := json.MarshalIndent(plan, "", "    ")
	if err != nil {
		return err.Error()
	}
	return string(out)
}
