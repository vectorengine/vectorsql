// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package datablocks

import (
	"expvar"
	"time"

	"datavalues"

	"base/metric"
)

func (block *DataBlock) Filter(checks []*datavalues.Value) error {
	defer expvar.Get(metric_datablock_filter_sec).(metric.Metric).Record(time.Now())

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
