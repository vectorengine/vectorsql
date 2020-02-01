// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package planners

import ()

type IPlan interface {
	Build() error
	Walk(visit Visit) error
	String() string
}

type Visit func(plan IPlan) (kontinue bool, err error)

func Walk(visit Visit, plans ...IPlan) error {
	for _, plan := range plans {
		if plan == nil {
			continue
		}
		kontinue, err := visit(plan)
		if err != nil {
			return err
		}
		if kontinue {
			err = plan.Walk(visit)
			if err != nil {
				return err
			}
		}
	}
	return nil
}
