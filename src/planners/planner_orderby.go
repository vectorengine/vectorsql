// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package planners

import (
	"fmt"
)

type OrderByPlan struct {
	Orders []Order
}

type Order struct {
	Expression IPlan
	Direction  string
}

func NewOrderByPlan(orders ...Order) *OrderByPlan {
	return &OrderByPlan{
		Orders: orders,
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
	res := "\n"
	res += "->"
	res += plan.Name()
	res += "\t--> "
	res += "["
	for _, expr := range plan.Orders {
		res += fmt.Sprintf("(field:%s, direction:%v)", expr.Expression, expr.Direction)
	}
	res += "]"
	return res
}
