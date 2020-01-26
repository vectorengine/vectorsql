// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package optimizers

import (
	"planners"
)

type Optimizer struct {
	Name        string
	Description string
	Reassembler func(planners.IPlan)
}

var DefaultOptimizers = []Optimizer{
	ProjectPushDownOptimizer,
	PredicatePushDownOptimizer,
}
