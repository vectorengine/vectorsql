// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package datablocks

import (
	"datatypes"
)

func (block *DataBlock) Filter(checks []datatypes.Value) error {
	// In place filter.
	for _, cv := range block.values {
		n := 0
		values := cv.values
		for i, check := range checks {
			if check.AsBool() {
				values[n] = values[i]
				n++
			}
		}
		cv.values = values[:n]
	}
	return nil
}
