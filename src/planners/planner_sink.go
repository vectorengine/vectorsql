// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package planners

import (
	"encoding/json"
)

type SinkPlan struct {
	Name string
}

func NewSinkPlan() *SinkPlan {
	return &SinkPlan{
		Name: "SinkPlan",
	}
}

func (plan *SinkPlan) Build() error {
	return nil
}

func (plan *SinkPlan) Walk(visit Visit) error {
	return nil
}

func (plan *SinkPlan) String() string {
	out, err := json.MarshalIndent(plan, "", "    ")
	if err != nil {
		return err.Error()
	}
	return string(out)
}
