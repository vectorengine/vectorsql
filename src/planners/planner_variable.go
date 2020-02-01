// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package planners

import (
	"encoding/json"
)

type VariablePlan struct {
	Name  string
	Value string
}

func NewVariablePlan(value string) *VariablePlan {
	return &VariablePlan{
		Name:  "VariablePlan",
		Value: value,
	}
}

func (plan *VariablePlan) Build() error {
	return nil
}

func (plan *VariablePlan) Walk(visit Visit) error {
	return nil
}

func (plan *VariablePlan) String() string {
	out, err := json.MarshalIndent(plan, "", "\t")
	if err != nil {
		return err.Error()
	}
	return string(out)
}
