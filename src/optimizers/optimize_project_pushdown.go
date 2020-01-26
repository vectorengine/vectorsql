// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package optimizers

import (
	"planners"
)

var ProjectPushDownOptimizer = Optimizer{
	Name:        "ProjectPushDownOptimizer",
	Description: "Push projects to scan plan",
	Reassembler: func(plan planners.IPlan) {
		var scan *planners.ScanPlan
		var project *planners.ProjectPlan

		visit := func(plan planners.IPlan) (kontinue bool, err error) {
			switch plan := plan.(type) {
			case *planners.ScanPlan:
				scan = plan
			case *planners.ProjectPlan:
				project = plan
			}
			return true, nil
		}
		if err := planners.Walk(visit, plan); err != nil {
			return
		}

		if scan != nil && project != nil {
			scan.Project = project
		}
	},
}
