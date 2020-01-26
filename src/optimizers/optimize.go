package optimizers

import (
	. "planners"
)

func Optimize(plan IPlan, optimizers []Optimizer) IPlan {
	for _, opt := range optimizers {
		opt.Reassembler(plan)
	}
	return plan
}
