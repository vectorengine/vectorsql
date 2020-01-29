// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package planners

import (
	"fmt"
)

type OrderByPlan struct {
	Expression []IPlan
	Directions []string
}

func NewOrderByPlan(exprs []IPlan, directions []string) *OrderByPlan {
	return &OrderByPlan{
		Expression: exprs,
		Directions: directions,
	}
}

func (plan *OrderByPlan) Name() string {
	return "OrderByNode"
}

func (plan *OrderByPlan) Build() error {
	return nil
}

func (plan *OrderByPlan) Walk(visit Visit) error {
	return nil
}

func (plan *OrderByPlan) String() string {
	res := plan.Name()
	res += "["
	for i, expr := range plan.Expression {
		res += fmt.Sprintf("(field:%s, direction:%v)", expr, plan.Directions[i])
	}
	res += "]"
	return res
}
