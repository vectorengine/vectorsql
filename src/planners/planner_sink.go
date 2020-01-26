// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package planners

import ()

type SinkPlan struct {
}

func NewSinkPlan() *SinkPlan {
	return &SinkPlan{}
}

func (plan *SinkPlan) Name() string {
	return "SinkNode"
}

func (plan *SinkPlan) Build() error {
	return nil
}

func (plan *SinkPlan) Walk(visit Visit) error {
	return nil
}

func (plan *SinkPlan) String() string {
	res := "\n"
	res += "->"
	res += plan.Name()
	res += "\t--> "
	return res
}
