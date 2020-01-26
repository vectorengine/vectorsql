// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package planners

import (
	"fmt"
)

type ScanPlan struct {
	Table   string
	Schema  string
	Filter  *FilterPlan
	Project *ProjectPlan
}

func NewScanPlan(table string, schema string) *ScanPlan {
	return &ScanPlan{
		Table:  table,
		Schema: schema,
	}
}

func (plan *ScanPlan) Name() string {
	return "ScanNode"
}

func (plan *ScanPlan) Build() error {
	return nil
}

func (plan *ScanPlan) Walk(visit Visit) error {
	return nil
}

func (plan *ScanPlan) String() string {
	res := "\n"
	res += "->"
	res += plan.Name()
	res += "\t--> "
	res += fmt.Sprintf("(table=[%v, %v])", plan.Schema, plan.Table)
	return res
}
