// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package planners

import (
	"encoding/json"
)

type Order struct {
	Expression IPlan
	Direction  string
}

type OrderByPlan struct {
	Name   string
	Orders []Order
}

func NewOrderByPlan(orders ...Order) *OrderByPlan {
	return &OrderByPlan{
		Name:   "OrderByPlan",
		Orders: orders,
	}
}

func (plan *OrderByPlan) Build() error {
	return nil
}

func (plan *OrderByPlan) Walk(visit Visit) error {
	return nil
}

func (plan *OrderByPlan) String() string {
	out, err := json.MarshalIndent(plan, "", "    ")
	if err != nil {
		return err.Error()
	}
	return string(out)
}
