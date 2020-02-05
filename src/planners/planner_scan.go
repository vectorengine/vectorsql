// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package planners

import (
	"encoding/json"
)

type ScanPlan struct {
	Name    string
	Table   string
	Schema  string
	Filter  *FilterPlan  `json:",omitempty"`
	Project *ProjectPlan `json:",omitempty"`
	Limit   *LimitPlan   `json:",omitempty"`
}

func NewScanPlan(table string, schema string) *ScanPlan {
	return &ScanPlan{
		Name:   "ScanPlan",
		Table:  table,
		Schema: schema,
	}
}

func (plan *ScanPlan) Build() error {
	return nil
}

func (plan *ScanPlan) Walk(visit Visit) error {
	return nil
}

func (plan *ScanPlan) String() string {
	out, err := json.MarshalIndent(plan, "", "\t")
	if err != nil {
		return err.Error()
	}
	return string(out)
}
