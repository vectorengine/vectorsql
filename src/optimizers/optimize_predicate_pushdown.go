// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package optimizers

import (
	"planners"
)

var PredicatePushDownOptimizer = Optimizer{
	Name:        "PredicatePushDownOptimizer",
	Description: "Push predicates to scan plan",
	Reassembler: func(plan planners.IPlan) {
		var scan *planners.ScanPlan
		var filter *planners.FilterPlan

		visit := func(plan planners.IPlan) (kontinue bool, err error) {
			switch plan := plan.(type) {
			case *planners.ScanPlan:
				scan = plan
			case *planners.FilterPlan:
				filter = plan
			}
			return true, nil
		}
		if err := planners.Walk(visit, plan); err != nil {
			return
		}

		if scan != nil && filter != nil {
			scan.Filter = filter
		}
	},
}
