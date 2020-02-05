// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package optimizers

import (
	"planners"
)

var LimitPushDownOptimizer = Optimizer{
	Name:        "LimitPushDownOptimizer",
	Description: "Push limit,offsets to scan plan",
	Reassembler: func(plan planners.IPlan) {
		var scan *planners.ScanPlan
		var limitPlan *planners.LimitPlan

		visit := func(plan planners.IPlan) (kontinue bool, err error) {
			switch plan := plan.(type) {
			case *planners.ScanPlan:
				scan = plan
			case *planners.LimitPlan:
				limitPlan = plan
			}
			return true, nil
		}

		if err := planners.Walk(visit, plan); err != nil {
			return
		}

		if scan != nil && limitPlan != nil {
			scan.Limit = limitPlan
		}
	},
}
